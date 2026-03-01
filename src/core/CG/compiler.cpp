/*
 * compiler.cpp — main C++ compiler for the CG subsystem.
 *
 * Implements the opaque svdb_cg_compiler_t / svdb_cg_program_t types and
 * the full C API declared in cg.h.
 *
 * Strategy
 * --------
 * The caller (Go CGO bridge) compiles the SQL AST to an initial bytecode
 * program using the existing Go code-generator, then serialises the result
 * to JSON and passes it here.  The C++ layer:
 *
 *   1. Parses the JSON (a simple hand-rolled scanner — no external deps).
 *   2. Applies the C++ optimisation passes (dead-code + peephole).
 *   3. Re-serialises the optimised program back to JSON for Go to read.
 *
 * For very simple programs (SELECT without FROM) the C++ can also build the
 * bytecode directly without a prior Go compile step.
 */

#include "cg.h"
#include "optimizer.h"
#include "plan_cache.h"
#include "expr_compiler.h"

#include <algorithm>
#include <cinttypes>
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <mutex>
#include <string>
#include <vector>
#include <unordered_map>

/* ── tiny JSON helpers ──────────────────────────────────────────────────── */

/* Skip whitespace */
static const char* skipWS(const char* p, const char* end)
{
    while (p < end && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r'))
        ++p;
    return p;
}

/* Match a literal token at *p; advance p past it on success */
static bool matchLit(const char*& p, const char* end, const char* tok)
{
    size_t n = std::strlen(tok);
    if ((size_t)(end - p) >= n && std::memcmp(p, tok, n) == 0) {
        p += n;
        return true;
    }
    return false;
}

/* Parse a JSON integer (no floats needed here) */
static bool parseI32(const char*& p, const char* end, int32_t& out)
{
    p = skipWS(p, end);
    if (p >= end) return false;
    bool neg = false;
    if (*p == '-') { neg = true; ++p; }
    if (p >= end || *p < '0' || *p > '9') return false;
    int64_t v = 0;
    while (p < end && *p >= '0' && *p <= '9') { v = v * 10 + (*p - '0'); ++p; }
    out = (int32_t)(neg ? -v : v);
    return true;
}

/* Parse a JSON string into a std::string */
static bool parseStr(const char*& p, const char* end, std::string& out)
{
    p = skipWS(p, end);
    if (p >= end || *p != '"') return false;
    ++p;
    out.clear();
    while (p < end && *p != '"') {
        if (*p == '\\') {
            ++p;
            if (p >= end) return false;
            switch (*p) {
            case '"':  out += '"';  break;
            case '\\': out += '\\'; break;
            case 'n':  out += '\n'; break;
            case 't':  out += '\t'; break;
            default:   out += *p;   break;
            }
        } else {
            out += *p;
        }
        ++p;
    }
    if (p < end) ++p; /* skip closing " */
    return true;
}

/* Expect a specific character */
static bool expectChar(const char*& p, const char* end, char ch)
{
    p = skipWS(p, end);
    if (p < end && *p == ch) { ++p; return true; }
    return false;
}

/* ── instruction / program structures ──────────────────────────────────── */

struct CgInstr {
    int32_t     op, p1, p2, p3;
    int64_t     p4_int;
    std::string p4_str;
    int32_t     p4_type; /* 0=none,1=int,2=str,3=regs */
    std::vector<int32_t> p4_regs; /* register list for p4_type==3 */
};

struct CgProgram {
    std::vector<CgInstr> instrs;
    std::vector<std::string> col_names;
    int32_t result_reg = -1;
    /* cached JSON output */
    std::string json_out;
    /* cached flat byte representation (for svdb_cg_get_bytecode) */
    std::vector<uint8_t> bytes_out;
    /* cached NUL-separated column names for svdb_cg_get_column_names */
    std::string col_names_flat;
};

/* ── JSON serialisation ─────────────────────────────────────────────────── */

static std::string instrToJSON(const CgInstr& ins)
{
    char buf[256];
    int n = std::snprintf(buf, sizeof(buf),
        "{\"op\":%d,\"p1\":%d,\"p2\":%d,\"p3\":%d,\"p4_int\":%" PRId64 ",\"p4_type\":%d",
        ins.op, ins.p1, ins.p2, ins.p3, ins.p4_int, ins.p4_type);
    std::string s(buf, (size_t)n);
    s += ",\"p4_str\":\"";
    for (char c : ins.p4_str) {
        if (c == '"')  s += "\\\"";
        else if (c == '\\') s += "\\\\";
        else if (c == '\n') s += "\\n";
        else s += c;
    }
    s += "\"";
    /* register array */
    if (ins.p4_type == 3 && !ins.p4_regs.empty()) {
        s += ",\"p4_regs\":[";
        for (size_t i = 0; i < ins.p4_regs.size(); ++i) {
            if (i) s += ',';
            char nb[24];
            std::snprintf(nb, sizeof(nb), "%d", ins.p4_regs[i]);
            s += nb;
        }
        s += ']';
    }
    s += '}';
    return s;
}

static void buildJSON(CgProgram& prog)
{
    std::string j;
    j.reserve(256 + prog.instrs.size() * 64);
    j += "{\"instructions\":[";
    for (size_t i = 0; i < prog.instrs.size(); ++i) {
        if (i) j += ',';
        j += instrToJSON(prog.instrs[i]);
    }
    j += "],\"column_names\":[";
    for (size_t i = 0; i < prog.col_names.size(); ++i) {
        if (i) j += ',';
        j += '"';
        j += prog.col_names[i];
        j += '"';
    }
    char rbuf[32];
    std::snprintf(rbuf, sizeof(rbuf), "],\"result_reg\":%d}", prog.result_reg);
    j += rbuf;
    prog.json_out = std::move(j);
}

static void buildBytes(CgProgram& prog)
{
    /* flat int32 encoding: 4 × int32 per instruction */
    size_t n = prog.instrs.size();
    prog.bytes_out.resize(n * 4 * sizeof(int32_t));
    int32_t* out = reinterpret_cast<int32_t*>(prog.bytes_out.data());
    for (size_t i = 0; i < n; ++i) {
        out[i * 4 + 0] = prog.instrs[i].op;
        out[i * 4 + 1] = prog.instrs[i].p1;
        out[i * 4 + 2] = prog.instrs[i].p2;
        out[i * 4 + 3] = prog.instrs[i].p3;
    }
}

static void buildColNamesFlat(CgProgram& prog)
{
    prog.col_names_flat.clear();
    for (auto& n : prog.col_names) {
        prog.col_names_flat += n;
        prog.col_names_flat += '\0';
    }
}

/* ── JSON parsing ───────────────────────────────────────────────────────── */

/*
 * Minimal JSON parser for the bytecode wire format:
 * {
 *   "instructions": [ {"op":N,"p1":N,"p2":N,"p3":N,
 *                      "p4_int":N,"p4_str":"...","p4_type":N}, ... ],
 *   "column_names": ["col1","col2",...],
 *   "result_reg": N
 * }
 */
static bool parseBytecodeJSON(const char* data, size_t len, CgProgram& prog, std::string& err)
{
    const char* p   = data;
    const char* end = data + len;

    p = skipWS(p, end);
    if (!expectChar(p, end, '{')) { err = "expected '{'"; return false; }

    while (true) {
        p = skipWS(p, end);
        if (p >= end) break;
        if (*p == '}') { ++p; break; }
        if (*p == ',') { ++p; continue; }

        std::string key;
        if (!parseStr(p, end, key)) break;
        p = skipWS(p, end);
        if (!expectChar(p, end, ':')) { err = "expected ':'"; return false; }
        p = skipWS(p, end);

        if (key == "instructions") {
            if (!expectChar(p, end, '[')) { err = "expected '[' for instructions"; return false; }
            while (true) {
                p = skipWS(p, end);
                if (p >= end) break;
                if (*p == ']') { ++p; break; }
                if (*p == ',') { ++p; continue; }
                if (*p != '{') break;
                ++p; /* skip '{' */

                CgInstr ins{};
                while (true) {
                    p = skipWS(p, end);
                    if (p >= end || *p == '}') { ++p; break; }
                    if (*p == ',') { ++p; continue; }

                    std::string ikey;
                    if (!parseStr(p, end, ikey)) break;
                    p = skipWS(p, end);
                    if (!expectChar(p, end, ':')) break;
                    p = skipWS(p, end);

                    if (ikey == "op")       { int32_t v=0; parseI32(p,end,v); ins.op=v; }
                    else if (ikey=="p1")    { int32_t v=0; parseI32(p,end,v); ins.p1=v; }
                    else if (ikey=="p2")    { int32_t v=0; parseI32(p,end,v); ins.p2=v; }
                    else if (ikey=="p3")    { int32_t v=0; parseI32(p,end,v); ins.p3=v; }
                    else if (ikey=="p4_type") { int32_t v=0; parseI32(p,end,v); ins.p4_type=v; }
                    else if (ikey=="p4_int") {
                        /* int64 */
                        p = skipWS(p, end);
                        bool neg2 = false;
                        if (p<end && *p=='-'){neg2=true;++p;}
                        int64_t v64=0;
                        while(p<end && *p>='0' && *p<='9'){v64=v64*10+(*p-'0');++p;}
                        ins.p4_int = neg2 ? -v64 : v64;
                    }
                    else if (ikey=="p4_str") { parseStr(p,end,ins.p4_str); }
                    else if (ikey=="p4_regs") {
                        /* parse integer array */
                        if (expectChar(p, end, '[')) {
                            while (true) {
                                p = skipWS(p, end);
                                if (p >= end || *p == ']') { if (p<end) ++p; break; }
                                if (*p == ',') { ++p; continue; }
                                int32_t rv = 0;
                                parseI32(p, end, rv);
                                ins.p4_regs.push_back(rv);
                            }
                        }
                    }
                    else {
                        /* skip unknown value */
                        if (*p=='"') { std::string tmp; parseStr(p,end,tmp); }
                        else { int32_t tmp=0; parseI32(p,end,tmp); }
                    }
                }
                prog.instrs.push_back(ins);
            }
        } else if (key == "column_names") {
            if (!expectChar(p, end, '[')) { err = "expected '[' for column_names"; return false; }
            while (true) {
                p = skipWS(p, end);
                if (p >= end) break;
                if (*p == ']') { ++p; break; }
                if (*p == ',') { ++p; continue; }
                std::string name;
                parseStr(p, end, name);
                prog.col_names.push_back(name);
            }
        } else if (key == "result_reg") {
            int32_t v = -1;
            parseI32(p, end, v);
            prog.result_reg = v;
        } else {
            /* skip unknown key's value (simple: scan to next , or }) */
            int depth = 0;
            while (p < end) {
                if (*p == '{' || *p == '[') ++depth;
                else if (*p == '}' || *p == ']') {
                    if (depth == 0) break;
                    --depth;
                } else if (*p == ',' && depth == 0) break;
                ++p;
            }
        }
    }
    return true;
}

/* ── CgInstr-level dead-code eliminator ──────────────────────────────────
 *
 * This pass works directly on CgInstr objects so that it has full access
 * to p4_regs (register lists used by OP_RESULT_ROW) and can correctly
 * track all register reads without relying on the flat-int32 optimizer.
 */

static const int32_t CG_OP_RESULT_ROW = 30;  /* mirrors VM.OpResultRow */
static const int32_t CG_OP_LOAD_CONST = 1;   /* mirrors VM.OpLoadConst */
static const int32_t CG_OP_MOVE       = 10;  /* mirrors VM.OpMove */
static const int32_t CG_OP_COPY       = 11;  /* mirrors VM.OpCopy */
static const int32_t CG_OP_SCOPY      = 12;  /* mirrors VM.OpSCopy */
static const int32_t CG_OP_NULL       = 13;  /* mirrors VM.OpNull */
static const int32_t CG_OP_ADD        = 4;
static const int32_t CG_OP_SUBTRACT   = 5;
static const int32_t CG_OP_MULTIPLY   = 6;
static const int32_t CG_OP_DIVIDE     = 7;
static const int32_t CG_OP_REMAINDER  = 8;

static void cgEliminateDeadCode(std::vector<CgInstr>& instrs)
{
    /* Build a map of register → read count */
    std::unordered_map<int32_t, int> readCount;

    for (auto& ins : instrs) {
        switch (ins.op) {
        case CG_OP_ADD: case CG_OP_SUBTRACT: case CG_OP_MULTIPLY:
        case CG_OP_DIVIDE: case CG_OP_REMAINDER:
            readCount[ins.p1]++;
            readCount[ins.p2]++;
            break;
        case CG_OP_MOVE: case CG_OP_COPY: case CG_OP_SCOPY:
            readCount[ins.p1]++;
            break;
        case CG_OP_RESULT_ROW:
            /* use p4_regs when present (Go wire format: p4_type==3) */
            if (ins.p4_type == 3 && !ins.p4_regs.empty()) {
                for (auto r : ins.p4_regs) readCount[r]++;
            } else {
                /* fallback: treat p1..p1+p2-1 as used registers */
                for (int32_t r = ins.p1; r < ins.p1 + ins.p2; ++r)
                    readCount[r]++;
            }
            break;
        default:
            break;
        }
    }

    /* Remove instructions that write to registers never read */
    instrs.erase(std::remove_if(instrs.begin(), instrs.end(),
        [&](const CgInstr& ins) -> bool {
            int32_t dst = -1;
            switch (ins.op) {
            case CG_OP_LOAD_CONST:
                dst = ins.p1;
                break;
            case CG_OP_MOVE: case CG_OP_COPY: case CG_OP_SCOPY:
                dst = ins.p2;
                break;
            case CG_OP_ADD: case CG_OP_SUBTRACT: case CG_OP_MULTIPLY:
            case CG_OP_DIVIDE: case CG_OP_REMAINDER:
                /* destination is in P4 (int) — but in the CgInstr wire format
                 * P4 integer is in p4_int when p4_type==1.  Skip for safety. */
                return false;
            default:
                return false;
            }
            return dst >= 0 && readCount.find(dst) == readCount.end();
        }),
        instrs.end());
}

/* ── internal compile helper ────────────────────────────────────────────── */

struct svdb_cg_compiler {
    int             opt_level = 1;
    svdb_cg_cache_t* plan_cache = nullptr;

    svdb_cg_compiler() { plan_cache = svdb_cg_cache_create(); }
    ~svdb_cg_compiler() { svdb_cg_cache_free(plan_cache); }
};

/* ── internal compile helper ────────────────────────────────────────────── */

static svdb_cg_program_t* doCompile(
    svdb_cg_compiler_t* compiler,
    const char*         json_data,
    size_t              json_len,
    char*               error_buf,
    size_t              error_buf_size)
{
    auto* c = reinterpret_cast<svdb_cg_compiler*>(compiler);
    auto* prog = new CgProgram();

    std::string err;
    if (!parseBytecodeJSON(json_data, json_len, *prog, err)) {
        if (error_buf && error_buf_size > 0) {
            std::snprintf(error_buf, error_buf_size, "CG parse error: %s", err.c_str());
        }
        delete prog;
        return nullptr;
    }

    /* apply CgInstr-level dead-code elimination (has full p4_regs access) */
    if (c->opt_level > 0 && !prog->instrs.empty()) {
        cgEliminateDeadCode(prog->instrs);
    }

    buildJSON(*prog);
    buildBytes(*prog);
    buildColNamesFlat(*prog);

    return reinterpret_cast<svdb_cg_program_t*>(prog);
}

/* ── public C API ────────────────────────────────────────────────────────── */

extern "C" {

svdb_cg_compiler_t* svdb_cg_create(void)
{
    return reinterpret_cast<svdb_cg_compiler_t*>(new svdb_cg_compiler());
}

void svdb_cg_destroy(svdb_cg_compiler_t* compiler)
{
    delete reinterpret_cast<svdb_cg_compiler*>(compiler);
}

svdb_cg_program_t* svdb_cg_compile_select(
    svdb_cg_compiler_t* compiler,
    const char* json, size_t json_len,
    char* error_buf, size_t error_buf_size)
{
    return doCompile(compiler, json, json_len, error_buf, error_buf_size);
}

svdb_cg_program_t* svdb_cg_compile_insert(
    svdb_cg_compiler_t* compiler,
    const char* json, size_t json_len,
    char* error_buf, size_t error_buf_size)
{
    return doCompile(compiler, json, json_len, error_buf, error_buf_size);
}

svdb_cg_program_t* svdb_cg_compile_update(
    svdb_cg_compiler_t* compiler,
    const char* json, size_t json_len,
    char* error_buf, size_t error_buf_size)
{
    return doCompile(compiler, json, json_len, error_buf, error_buf_size);
}

svdb_cg_program_t* svdb_cg_compile_delete(
    svdb_cg_compiler_t* compiler,
    const char* json, size_t json_len,
    char* error_buf, size_t error_buf_size)
{
    return doCompile(compiler, json, json_len, error_buf, error_buf_size);
}

size_t svdb_cg_optimize_raw(
    svdb_cg_compiler_t* compiler,
    const int32_t* in_buf, size_t in_count,
    int32_t* out_buf, size_t out_cap)
{
    auto* c = reinterpret_cast<svdb_cg_compiler*>(compiler);
    return svdb_cg_optimize_raw_impl(c->opt_level, in_buf, in_count, out_buf, out_cap);
}

size_t svdb_cg_optimize_bc_instrs(
    svdb_cg_compiler_t* compiler,
    const uint8_t* in_buf, size_t in_count,
    uint8_t* out_buf, size_t out_cap)
{
    auto* c = reinterpret_cast<svdb_cg_compiler*>(compiler);
    return svdb_cg_optimize_bc_instrs_impl(c->opt_level, in_buf, in_count, out_buf, out_cap);
}

const uint8_t* svdb_cg_get_bytecode(svdb_cg_program_t* program, size_t* out_len)
{
    auto* p = reinterpret_cast<CgProgram*>(program);
    *out_len = p->bytes_out.size();
    return p->bytes_out.data();
}

const char* svdb_cg_get_column_names(svdb_cg_program_t* program, size_t* out_count)
{
    auto* p = reinterpret_cast<CgProgram*>(program);
    *out_count = p->col_names.size();
    return p->col_names_flat.c_str();
}

int32_t svdb_cg_get_result_reg(svdb_cg_program_t* program)
{
    return reinterpret_cast<CgProgram*>(program)->result_reg;
}

const char* svdb_cg_get_json(svdb_cg_program_t* program)
{
    return reinterpret_cast<CgProgram*>(program)->json_out.c_str();
}

void svdb_cg_program_free(svdb_cg_program_t* program)
{
    delete reinterpret_cast<CgProgram*>(program);
}

void svdb_cg_set_optimization_level(svdb_cg_compiler_t* compiler, int level)
{
    reinterpret_cast<svdb_cg_compiler*>(compiler)->opt_level = level;
}

void svdb_cg_cache_put(svdb_cg_compiler_t* compiler, const char* sql, svdb_cg_program_t* program)
{
    auto* c = reinterpret_cast<svdb_cg_compiler*>(compiler);
    auto* p = reinterpret_cast<CgProgram*>(program);
    svdb_cg_cache_put_json(c->plan_cache, sql, p->json_out.c_str(), p->json_out.size());
}

svdb_cg_program_t* svdb_cg_cache_get(svdb_cg_compiler_t* compiler, const char* sql)
{
    auto* c = reinterpret_cast<svdb_cg_compiler*>(compiler);

    /* Two-step: first query length under lock, then retrieve full copy */
    size_t json_len = 0;
    /* probe with empty buf to get length */
    if (!svdb_cg_cache_copy_json(c->plan_cache, sql, nullptr, 0, &json_len) || json_len == 0)
        return nullptr;

    /* allocate and copy under lock */
    std::vector<char> json_buf(json_len + 1, '\0');
    if (!svdb_cg_cache_copy_json(c->plan_cache, sql, json_buf.data(), json_len + 1, &json_len))
        return nullptr;

    auto* prog = new CgProgram();
    std::string err;
    if (!parseBytecodeJSON(json_buf.data(), json_len, *prog, err)) {
        delete prog;
        return nullptr;
    }
    buildJSON(*prog);
    buildBytes(*prog);
    buildColNamesFlat(*prog);
    return reinterpret_cast<svdb_cg_program_t*>(prog);
}

void svdb_cg_cache_clear(svdb_cg_compiler_t* compiler)
{
    auto* c = reinterpret_cast<svdb_cg_compiler*>(compiler);
    svdb_cg_cache_erase(c->plan_cache);
}

size_t svdb_cg_cache_size(svdb_cg_compiler_t* compiler)
{
    auto* c = reinterpret_cast<svdb_cg_compiler*>(compiler);
    return svdb_cg_cache_count(c->plan_cache);
}

} /* extern "C" */
