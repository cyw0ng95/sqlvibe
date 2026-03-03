/*
 * vm_opcode.cpp — pure register-to-register opcode dispatch for the
 * sqlvibe VM.  Every opcode handled here operates solely on svdb_value_t
 * values; no VM state, cursor, or storage access is required.
 *
 * Consumed by the Go layer via CGO (internal/VM/vm_opcode_cgo.go).
 */

#include "vm_opcode.h"
#include "string_funcs.h"
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <ctime>

/* ── Value helpers ──────────────────────────────────────────────────────── */

static svdb_value_t make_null(void) {
    svdb_value_t v;
    memset(&v, 0, sizeof(v));
    v.val_type = SVDB_VAL_NULL;
    return v;
}

static svdb_value_t make_int(int64_t i) {
    svdb_value_t v;
    memset(&v, 0, sizeof(v));
    v.val_type = SVDB_VAL_INT;
    v.int_val  = i;
    return v;
}

static svdb_value_t make_float(double f) {
    svdb_value_t v;
    memset(&v, 0, sizeof(v));
    v.val_type  = SVDB_VAL_FLOAT;
    v.float_val = f;
    return v;
}

/* Allocates a heap copy of the string for TEXT results. */
static svdb_value_t make_text(const char* s, size_t len) {
    svdb_value_t v;
    memset(&v, 0, sizeof(v));
    v.val_type = SVDB_VAL_TEXT;
    if (s && len > 0) {
        char* p = (char*)malloc(len + 1);
        if (p) {
            memcpy(p, s, len);
            p[len] = '\0';
            v.str_data = p;
            v.str_len  = len;
        }
    } else {
        /* empty string */
        char* p = (char*)malloc(1);
        if (p) { p[0] = '\0'; }
        v.str_data = p;
        v.str_len  = 0;
    }
    return v;
}

static double val_to_double(const svdb_value_t* v) {
    if (!v) return 0.0;
    switch (v->val_type) {
        case SVDB_VAL_INT:   return (double)v->int_val;
        case SVDB_VAL_FLOAT: return v->float_val;
        case SVDB_VAL_TEXT:
            if (v->str_data && v->str_len > 0) {
                return strtod(v->str_data, nullptr);
            }
            return 0.0;
        default: return 0.0;
    }
}

static int64_t val_to_int64(const svdb_value_t* v) {
    if (!v) return 0;
    switch (v->val_type) {
        case SVDB_VAL_INT:   return v->int_val;
        case SVDB_VAL_FLOAT: return (int64_t)v->float_val;
        case SVDB_VAL_TEXT:
            if (v->str_data && v->str_len > 0) {
                return (int64_t)strtoll(v->str_data, nullptr, 10);
            }
            return 0;
        default: return 0;
    }
}

static int val_is_numeric(const svdb_value_t* v) {
    return v && (v->val_type == SVDB_VAL_INT || v->val_type == SVDB_VAL_FLOAT);
}

/* Compare two non-NULL values. Returns -1, 0, or 1. */
static int val_compare(const svdb_value_t* a, const svdb_value_t* b) {
    if (!a || !b) return 0;

    /* Both INT */
    if (a->val_type == SVDB_VAL_INT && b->val_type == SVDB_VAL_INT) {
        if (a->int_val < b->int_val) return -1;
        if (a->int_val > b->int_val) return  1;
        return 0;
    }
    /* Numeric comparison (at least one FLOAT) */
    if ((a->val_type == SVDB_VAL_INT || a->val_type == SVDB_VAL_FLOAT) &&
        (b->val_type == SVDB_VAL_INT || b->val_type == SVDB_VAL_FLOAT)) {
        double da = val_to_double(a);
        double db = val_to_double(b);
        if (da < db) return -1;
        if (da > db) return  1;
        return 0;
    }
    /* TEXT comparison */
    if (a->val_type == SVDB_VAL_TEXT && b->val_type == SVDB_VAL_TEXT) {
        size_t la = a->str_len, lb = b->str_len;
        int r = memcmp(a->str_data ? a->str_data : "",
                       b->str_data ? b->str_data : "",
                       la < lb ? la : lb);
        if (r != 0) return r < 0 ? -1 : 1;
        if (la < lb) return -1;
        if (la > lb) return  1;
        return 0;
    }
    /* Mixed type ordering: NULL < INT < FLOAT < TEXT < BLOB */
    static const int type_order[] = { 0, 1, 2, 3, 4, 1 }; /* indexed by val_type */
    int oa = (a->val_type < 6) ? type_order[a->val_type] : 3;
    int ob = (b->val_type < 6) ? type_order[b->val_type] : 3;
    if (oa < ob) return -1;
    if (oa > ob) return  1;
    return 0;
}

/* ── Arithmetic helpers ─────────────────────────────────────────────────── */

static svdb_value_t arith_add(const svdb_value_t* a, const svdb_value_t* b) {
    if (!a || !b || a->val_type == SVDB_VAL_NULL || b->val_type == SVDB_VAL_NULL)
        return make_null();
    if (a->val_type == SVDB_VAL_INT && b->val_type == SVDB_VAL_INT)
        return make_int(a->int_val + b->int_val);
    return make_float(val_to_double(a) + val_to_double(b));
}

static svdb_value_t arith_sub(const svdb_value_t* a, const svdb_value_t* b) {
    if (!a || !b || a->val_type == SVDB_VAL_NULL || b->val_type == SVDB_VAL_NULL)
        return make_null();
    if (a->val_type == SVDB_VAL_INT && b->val_type == SVDB_VAL_INT)
        return make_int(a->int_val - b->int_val);
    return make_float(val_to_double(a) - val_to_double(b));
}

static svdb_value_t arith_mul(const svdb_value_t* a, const svdb_value_t* b) {
    if (!a || !b || a->val_type == SVDB_VAL_NULL || b->val_type == SVDB_VAL_NULL)
        return make_null();
    if (a->val_type == SVDB_VAL_INT && b->val_type == SVDB_VAL_INT)
        return make_int(a->int_val * b->int_val);
    return make_float(val_to_double(a) * val_to_double(b));
}

static svdb_value_t arith_div(const svdb_value_t* a, const svdb_value_t* b) {
    if (!a || !b || a->val_type == SVDB_VAL_NULL || b->val_type == SVDB_VAL_NULL)
        return make_null();
    if (a->val_type == SVDB_VAL_INT && b->val_type == SVDB_VAL_INT) {
        if (b->int_val == 0) return make_null();
        return make_int(a->int_val / b->int_val);
    }
    double db = val_to_double(b);
    if (db == 0.0) return make_null();
    return make_float(val_to_double(a) / db);
}

static svdb_value_t arith_rem(const svdb_value_t* a, const svdb_value_t* b) {
    if (!a || !b || a->val_type == SVDB_VAL_NULL || b->val_type == SVDB_VAL_NULL)
        return make_null();
    if (a->val_type == SVDB_VAL_INT && b->val_type == SVDB_VAL_INT) {
        if (b->int_val == 0) return make_null();
        return make_int(a->int_val % b->int_val);
    }
    double db = val_to_double(b);
    if (db == 0.0) return make_null();
    return make_float(fmod(val_to_double(a), db));
}

/* ── Pattern matching helpers ───────────────────────────────────────────── */

/* ASCII-safe case fold: only lower ASCII letters, everything else unchanged. */
static unsigned char ascii_lower(unsigned char c) {
    return (c >= 'A' && c <= 'Z') ? (c + 32) : c;
}

/* SQLite-style LIKE: % matches any sequence, _ matches one char.
 * Case-insensitive for ASCII letters only.  \ is the escape character
 * (matches the next character literally, suppressing its wildcard meaning). */
static int like_match(const char* str, size_t slen,
                      const char* pat, size_t plen) {
    /* Recursive backtracking implementation matching likeMatchRecursive in Go. */
    if (plen == 0) return (slen == 0) ? 1 : 0;
    if (slen == 0) {
        /* Pattern must consist entirely of % to match empty string. */
        size_t i = 0;
        while (i < plen) {
            if (pat[i] == '\\' && i + 1 < plen) { return 0; } /* escaped char requires input */
            if (pat[i] != '%') return 0;
            ++i;
        }
        return 1;
    }
    unsigned char pc = (unsigned char)pat[0];
    if (pc == '%') {
        /* Try zero characters, then advance str by one and retry. */
        if (like_match(str, slen, pat + 1, plen - 1)) return 1;
        return like_match(str + 1, slen - 1, pat, plen);
    }
    if (pc == '_') {
        /* Any single character. */
        return like_match(str + 1, slen - 1, pat + 1, plen - 1);
    }
    if (pc == '\\' && plen >= 2) {
        /* Escape: next pattern char is literal. */
        unsigned char literal = (unsigned char)pat[1];
        if (ascii_lower((unsigned char)str[0]) == ascii_lower(literal))
            return like_match(str + 1, slen - 1, pat + 2, plen - 2);
        return 0;
    }
    /* Normal character: case-insensitive compare. */
    if (ascii_lower((unsigned char)str[0]) == ascii_lower(pc))
        return like_match(str + 1, slen - 1, pat + 1, plen - 1);
    return 0;
}

/* Simple GLOB: * matches any sequence, ? matches one char, case-sensitive. */
static int glob_match(const char* str, size_t slen,
                      const char* pat, size_t plen) {
    size_t si = 0, pi = 0;
    size_t star_pi = (size_t)-1, star_si = 0;
    while (si < slen) {
        if (pi < plen && pat[pi] == '*') {
            star_pi = ++pi;
            star_si = si;
        } else if (pi < plen && (pat[pi] == '?' || pat[pi] == str[si])) {
            ++pi; ++si;
        } else if (star_pi != (size_t)-1) {
            pi = star_pi;
            si = ++star_si;
        } else {
            return 0;
        }
    }
    while (pi < plen && pat[pi] == '*') ++pi;
    return (pi == plen) ? 1 : 0;
}

/* ── String helpers ─────────────────────────────────────────────────────── */

/* Count UTF-8 characters (runes) in a byte string, matching Go's
 * utf8.RuneCountInString semantics. */
static size_t utf8_char_count(const char* s, size_t bytes) {
    size_t n = 0;
    for (size_t i = 0; i < bytes; ++i) {
        /* Count only leading bytes (not continuation bytes 10xxxxxx). */
        if (((unsigned char)s[i] & 0xC0) != 0x80) ++n;
    }
    return n;
}

static const char* val_str(const svdb_value_t* v, size_t* out_len) {
    if (!v || v->val_type != SVDB_VAL_TEXT || !v->str_data) {
        *out_len = 0;
        return "";
    }
    *out_len = v->str_len;
    return v->str_data;
}

/* Convert any value to a heap-allocated string representation. */
static char* val_to_string(const svdb_value_t* v, size_t* out_len) {
    if (!v || v->val_type == SVDB_VAL_NULL) {
        *out_len = 0;
        return nullptr;
    }
    char buf[64];
    switch (v->val_type) {
        case SVDB_VAL_INT:
            snprintf(buf, sizeof(buf), "%lld", (long long)v->int_val);
            break;
        case SVDB_VAL_FLOAT: {
            /* Match Go's strconv.FormatFloat 'f' -1 64 behaviour: minimal digits */
            snprintf(buf, sizeof(buf), "%g", v->float_val);
            break;
        }
        case SVDB_VAL_TEXT:
            if (v->str_data) {
                *out_len = v->str_len;
                char* p = (char*)malloc(v->str_len + 1);
                if (p) {
                    memcpy(p, v->str_data, v->str_len);
                    p[v->str_len] = '\0';
                }
                return p;
            }
            *out_len = 0;
            return nullptr;
        case SVDB_VAL_BLOB:
            snprintf(buf, sizeof(buf), "(blob)");
            break;
        default:
            snprintf(buf, sizeof(buf), "");
    }
    size_t n = strlen(buf);
    char* p = (char*)malloc(n + 1);
    if (p) { memcpy(p, buf, n + 1); }
    *out_len = n;
    return p;
}

/* ── typeof string constants (static storage, no alloc needed) ──────────── */

static const char* typeof_name(int32_t vt) {
    switch (vt) {
        case SVDB_VAL_INT:   return "integer";
        case SVDB_VAL_FLOAT: return "real";
        case SVDB_VAL_TEXT:  return "text";
        case SVDB_VAL_BLOB:  return "blob";
        default:             return "null";
    }
}

/* ── Public API ─────────────────────────────────────────────────────────── */

extern "C" {

void svdb_pure_value_free(svdb_value_t* v) {
    if (!v) return;
    if ((v->val_type == SVDB_VAL_TEXT || v->val_type == SVDB_VAL_BLOB) && v->str_data) {
        free((void*)v->str_data);
        v->str_data = nullptr;
        v->str_len  = 0;
    }
}

int32_t svdb_vm_dispatch_pure(
    int32_t             opcode,
    const svdb_value_t* v1,
    const svdb_value_t* v2,
    const svdb_value_t* v3,
    int64_t             aux_i,
    const char*         aux_s,
    size_t              aux_len,
    svdb_value_t*       out_val)
{
    if (!out_val) return SVDB_PURE_ERROR;

    switch (opcode) {

    /* ── NULL assignment ──────────────────────────────────────────────── */
    case SVDB_OP_NULL:
    case SVDB_OP_CONST_NULL:
        *out_val = make_null();
        return SVDB_PURE_OK;

    /* ── Register copy ────────────────────────────────────────────────── */
    case SVDB_OP_MOVE:
    case SVDB_OP_COPY:
    case SVDB_OP_SCOPY:
        if (!v1) { *out_val = make_null(); return SVDB_PURE_OK; }
        if (v1->val_type == SVDB_VAL_TEXT && v1->str_data) {
            *out_val = make_text(v1->str_data, v1->str_len);
        } else {
            *out_val = *v1;
            out_val->str_data  = nullptr;
            out_val->str_len   = 0;
            out_val->bytes_data = nullptr;
            out_val->bytes_len  = 0;
        }
        return SVDB_PURE_OK;

    case SVDB_OP_INT_COPY:
        if (!v1) { *out_val = make_null(); return SVDB_PURE_OK; }
        if (v1->val_type == SVDB_VAL_INT)
            *out_val = make_int(v1->int_val);
        else if (v1->val_type == SVDB_VAL_FLOAT)
            *out_val = make_int((int64_t)v1->float_val);
        else
            *out_val = make_null();
        return SVDB_PURE_OK;

    /* ── Arithmetic ───────────────────────────────────────────────────── */
    case SVDB_OP_ADD:       *out_val = arith_add(v1, v2); return SVDB_PURE_OK;
    case SVDB_OP_SUBTRACT:  *out_val = arith_sub(v1, v2); return SVDB_PURE_OK;
    case SVDB_OP_MULTIPLY:  *out_val = arith_mul(v1, v2); return SVDB_PURE_OK;
    case SVDB_OP_DIVIDE:    *out_val = arith_div(v1, v2); return SVDB_PURE_OK;
    case SVDB_OP_REMAINDER: *out_val = arith_rem(v1, v2); return SVDB_PURE_OK;
    case SVDB_OP_MOD:       *out_val = arith_rem(v1, v2); return SVDB_PURE_OK;

    case SVDB_OP_ADD_IMM:
        if (!v1 || v1->val_type == SVDB_VAL_NULL) { *out_val = make_null(); return SVDB_PURE_OK; }
        if (v1->val_type == SVDB_VAL_INT)
            *out_val = make_int(v1->int_val + aux_i);
        else
            *out_val = make_float(val_to_double(v1) + (double)aux_i);
        return SVDB_PURE_OK;

    case SVDB_OP_BIT_AND:
        if (!v1 || !v2 || v1->val_type == SVDB_VAL_NULL || v2->val_type == SVDB_VAL_NULL) {
            *out_val = make_null(); return SVDB_PURE_OK;
        }
        *out_val = make_int(val_to_int64(v1) & val_to_int64(v2));
        return SVDB_PURE_OK;

    case SVDB_OP_BIT_OR:
        if (!v1 || !v2 || v1->val_type == SVDB_VAL_NULL || v2->val_type == SVDB_VAL_NULL) {
            *out_val = make_null(); return SVDB_PURE_OK;
        }
        *out_val = make_int(val_to_int64(v1) | val_to_int64(v2));
        return SVDB_PURE_OK;

    case SVDB_OP_SHIFT_LEFT:
        if (!v1 || !v2 || v1->val_type == SVDB_VAL_NULL || v2->val_type == SVDB_VAL_NULL) {
            *out_val = make_null(); return SVDB_PURE_OK;
        }
        {
            int64_t shift = val_to_int64(v2);
            if (shift < 0 || shift >= 64) { *out_val = make_int(0); return SVDB_PURE_OK; }
            *out_val = make_int(val_to_int64(v1) << shift);
        }
        return SVDB_PURE_OK;

    case SVDB_OP_SHIFT_RIGHT:
        if (!v1 || !v2 || v1->val_type == SVDB_VAL_NULL || v2->val_type == SVDB_VAL_NULL) {
            *out_val = make_null(); return SVDB_PURE_OK;
        }
        {
            int64_t shift = val_to_int64(v2);
            if (shift < 0 || shift >= 64) { *out_val = make_int(0); return SVDB_PURE_OK; }
            /* Arithmetic (signed) right shift */
            *out_val = make_int(val_to_int64(v1) >> shift);
        }
        return SVDB_PURE_OK;

    /* ── Comparison ───────────────────────────────────────────────────── */
    case SVDB_OP_EQ:
        if (!v1 || !v2 || v1->val_type == SVDB_VAL_NULL || v2->val_type == SVDB_VAL_NULL) {
            *out_val = make_null(); return SVDB_PURE_OK;
        }
        *out_val = make_int(val_compare(v1, v2) == 0 ? 1 : 0);
        return SVDB_PURE_OK;

    case SVDB_OP_NE:
        if (!v1 || !v2 || v1->val_type == SVDB_VAL_NULL || v2->val_type == SVDB_VAL_NULL) {
            *out_val = make_null(); return SVDB_PURE_OK;
        }
        *out_val = make_int(val_compare(v1, v2) != 0 ? 1 : 0);
        return SVDB_PURE_OK;

    case SVDB_OP_LT:
        if (!v1 || !v2 || v1->val_type == SVDB_VAL_NULL || v2->val_type == SVDB_VAL_NULL) {
            *out_val = make_null(); return SVDB_PURE_OK;
        }
        *out_val = make_int(val_compare(v1, v2) < 0 ? 1 : 0);
        return SVDB_PURE_OK;

    case SVDB_OP_LE:
        if (!v1 || !v2 || v1->val_type == SVDB_VAL_NULL || v2->val_type == SVDB_VAL_NULL) {
            *out_val = make_null(); return SVDB_PURE_OK;
        }
        *out_val = make_int(val_compare(v1, v2) <= 0 ? 1 : 0);
        return SVDB_PURE_OK;

    case SVDB_OP_GT:
        if (!v1 || !v2 || v1->val_type == SVDB_VAL_NULL || v2->val_type == SVDB_VAL_NULL) {
            *out_val = make_null(); return SVDB_PURE_OK;
        }
        *out_val = make_int(val_compare(v1, v2) > 0 ? 1 : 0);
        return SVDB_PURE_OK;

    case SVDB_OP_GE:
        if (!v1 || !v2 || v1->val_type == SVDB_VAL_NULL || v2->val_type == SVDB_VAL_NULL) {
            *out_val = make_null(); return SVDB_PURE_OK;
        }
        *out_val = make_int(val_compare(v1, v2) >= 0 ? 1 : 0);
        return SVDB_PURE_OK;

    case SVDB_OP_IS:
        /* IS: NULL IS NULL = true, NULL IS non-NULL = false */
        if (!v1 || !v2) { *out_val = make_null(); return SVDB_PURE_OK; }
        if (v1->val_type == SVDB_VAL_NULL && v2->val_type == SVDB_VAL_NULL)
            *out_val = make_int(1);
        else if (v1->val_type == SVDB_VAL_NULL || v2->val_type == SVDB_VAL_NULL)
            *out_val = make_int(0);
        else
            *out_val = make_int(val_compare(v1, v2) == 0 ? 1 : 0);
        return SVDB_PURE_OK;

    case SVDB_OP_IS_NOT:
        if (!v1 || !v2) { *out_val = make_null(); return SVDB_PURE_OK; }
        if (v1->val_type == SVDB_VAL_NULL && v2->val_type == SVDB_VAL_NULL)
            *out_val = make_int(0);
        else if (v1->val_type == SVDB_VAL_NULL || v2->val_type == SVDB_VAL_NULL)
            *out_val = make_int(1);
        else
            *out_val = make_int(val_compare(v1, v2) != 0 ? 1 : 0);
        return SVDB_PURE_OK;

    case SVDB_OP_IS_NULL:
        *out_val = make_int((!v1 || v1->val_type == SVDB_VAL_NULL) ? 1 : 0);
        return SVDB_PURE_OK;

    case SVDB_OP_NOT_NULL:
        *out_val = make_int((v1 && v1->val_type != SVDB_VAL_NULL) ? 1 : 0);
        return SVDB_PURE_OK;

    /* IfNull2: return v2 if v1 is NULL, otherwise return v1 */
    case SVDB_OP_IF_NULL2:
        if (!v1 || v1->val_type == SVDB_VAL_NULL) {
            if (!v2) { *out_val = make_null(); }
            else if (v2->val_type == SVDB_VAL_TEXT && v2->str_data)
                *out_val = make_text(v2->str_data, v2->str_len);
            else
                *out_val = *v2;
        } else {
            if (v1->val_type == SVDB_VAL_TEXT && v1->str_data)
                *out_val = make_text(v1->str_data, v1->str_len);
            else
                *out_val = *v1;
        }
        return SVDB_PURE_OK;

    /* ── String operations ────────────────────────────────────────────── */
    case SVDB_OP_CONCAT: {
        if (!v1 || !v2) { *out_val = make_null(); return SVDB_PURE_OK; }
        if (v1->val_type == SVDB_VAL_NULL || v2->val_type == SVDB_VAL_NULL) {
            *out_val = make_null(); return SVDB_PURE_OK;
        }
        size_t la = 0, lb = 0;
        /* convert both to text */
        char* sa = val_to_string(v1, &la);
        char* sb = val_to_string(v2, &lb);
        char* p = (char*)malloc(la + lb + 1);
        if (p) {
            if (sa && la) memcpy(p, sa, la);
            if (sb && lb) memcpy(p + la, sb, lb);
            p[la + lb] = '\0';
        }
        free(sa); free(sb);
        *out_val = make_null();
        if (p) {
            out_val->val_type = SVDB_VAL_TEXT;
            out_val->str_data = p;
            out_val->str_len  = la + lb;
        }
        return SVDB_PURE_OK;
    }

    case SVDB_OP_LENGTH: {
        if (!v1 || v1->val_type == SVDB_VAL_NULL) { *out_val = make_null(); return SVDB_PURE_OK; }
        if (v1->val_type == SVDB_VAL_TEXT) {
            /* SQL LENGTH returns character count (UTF-8 runes), not byte length. */
            size_t nc = v1->str_data ? utf8_char_count(v1->str_data, v1->str_len) : 0;
            *out_val = make_int((int64_t)nc);
        } else if (v1->val_type == SVDB_VAL_BLOB) {
            /* BLOB: str_data/str_len used by pureGoValToC for byte data. */
            *out_val = make_int((int64_t)v1->str_len);
        } else {
            /* Numeric → length of its string representation. */
            size_t slen = 0;
            char* s = val_to_string(v1, &slen);
            free(s);
            *out_val = make_int((int64_t)slen);
        }
        return SVDB_PURE_OK;
    }

    case SVDB_OP_UPPER: {
        if (!v1 || v1->val_type == SVDB_VAL_NULL) { *out_val = make_null(); return SVDB_PURE_OK; }
        size_t slen = 0;
        char* s = val_to_string(v1, &slen);
        if (s && slen) { svdb_str_upper(s, slen); }
        *out_val = make_null();
        if (s) {
            out_val->val_type = SVDB_VAL_TEXT;
            out_val->str_data = s;
            out_val->str_len  = slen;
        }
        return SVDB_PURE_OK;
    }

    case SVDB_OP_LOWER: {
        if (!v1 || v1->val_type == SVDB_VAL_NULL) { *out_val = make_null(); return SVDB_PURE_OK; }
        size_t slen = 0;
        char* s = val_to_string(v1, &slen);
        if (s && slen) { svdb_str_lower(s, slen); }
        *out_val = make_null();
        if (s) {
            out_val->val_type = SVDB_VAL_TEXT;
            out_val->str_data = s;
            out_val->str_len  = slen;
        }
        return SVDB_PURE_OK;
    }

    case SVDB_OP_TRIM:
    case SVDB_OP_LTRIM:
    case SVDB_OP_RTRIM: {
        if (!v1 || v1->val_type == SVDB_VAL_NULL) { *out_val = make_null(); return SVDB_PURE_OK; }
        size_t slen = v1->str_len;
        const char* sptr = (v1->val_type == SVDB_VAL_TEXT && v1->str_data) ? v1->str_data : "";
        int do_left  = (opcode == SVDB_OP_TRIM || opcode == SVDB_OP_LTRIM);
        int do_right = (opcode == SVDB_OP_TRIM || opcode == SVDB_OP_RTRIM);
        /* Use custom character set from v2 if provided, otherwise trim spaces. */
        const char* chars     = " ";
        size_t      chars_len = 1;
        if (v2 && v2->val_type == SVDB_VAL_TEXT && v2->str_data && v2->str_len > 0) {
            chars     = v2->str_data;
            chars_len = v2->str_len;
        }
        size_t start = 0, end = slen;
        if (do_left) {
            while (start < end) {
                int in_set = 0;
                for (size_t ci = 0; ci < chars_len && !in_set; ++ci)
                    if (sptr[start] == chars[ci]) in_set = 1;
                if (!in_set) break;
                ++start;
            }
        }
        if (do_right) {
            while (end > start) {
                int in_set = 0;
                for (size_t ci = 0; ci < chars_len && !in_set; ++ci)
                    if (sptr[end - 1] == chars[ci]) in_set = 1;
                if (!in_set) break;
                --end;
            }
        }
        *out_val = make_text(sptr + start, end - start);
        return SVDB_PURE_OK;
    }

    case SVDB_OP_INSTR: {
        if (!v1 || !v2 || v1->val_type == SVDB_VAL_NULL || v2->val_type == SVDB_VAL_NULL) {
            *out_val = make_null(); return SVDB_PURE_OK;
        }
        size_t hlen = v1->str_len, nlen = v2->str_len;
        const char* h = (v1->str_data) ? v1->str_data : "";
        const char* n = (v2->str_data) ? v2->str_data : "";
        if (nlen == 0) { *out_val = make_int(1); return SVDB_PURE_OK; }
        /* Find first occurrence */
        int64_t pos = 0;
        for (size_t i = 0; i + nlen <= hlen; ++i) {
            if (memcmp(h + i, n, nlen) == 0) { pos = (int64_t)(i + 1); break; }
        }
        *out_val = make_int(pos);
        return SVDB_PURE_OK;
    }

    case SVDB_OP_LIKE:
    case SVDB_OP_NOT_LIKE: {
        if (!v1 || !v2 || v1->val_type == SVDB_VAL_NULL || v2->val_type == SVDB_VAL_NULL) {
            *out_val = make_null(); return SVDB_PURE_OK;
        }
        /* Convert the subject (v1) to a string when it is not already TEXT.
         * This mirrors Go's fmt.Sprintf("%v", ...) for integer/float columns. */
        char* tmp_str = nullptr;
        size_t tmp_len = 0;
        const char* str;
        size_t slen;
        if (v1->val_type == SVDB_VAL_TEXT) {
            str  = v1->str_data ? v1->str_data : "";
            slen = v1->str_len;
        } else {
            tmp_str = val_to_string(v1, &tmp_len);
            str  = tmp_str ? tmp_str : "";
            slen = tmp_len;
        }
        const char* pat = (v2->str_data) ? v2->str_data : "";
        size_t plen = v2->str_len;
        int matched = like_match(str, slen, pat, plen);
        if (tmp_str) free(tmp_str);
        if (opcode == SVDB_OP_NOT_LIKE) matched = !matched;
        *out_val = make_int(matched ? 1 : 0);
        return SVDB_PURE_OK;
    }

    case SVDB_OP_GLOB: {
        if (!v1 || !v2 || v1->val_type == SVDB_VAL_NULL || v2->val_type == SVDB_VAL_NULL) {
            *out_val = make_null(); return SVDB_PURE_OK;
        }
        char* tmp_str = nullptr;
        size_t tmp_len = 0;
        const char* str;
        size_t slen;
        if (v1->val_type == SVDB_VAL_TEXT) {
            str  = v1->str_data ? v1->str_data : "";
            slen = v1->str_len;
        } else {
            tmp_str = val_to_string(v1, &tmp_len);
            str  = tmp_str ? tmp_str : "";
            slen = tmp_len;
        }
        const char* pat = (v2->str_data) ? v2->str_data : "";
        int matched = glob_match(str, slen, pat, v2->str_len);
        if (tmp_str) free(tmp_str);
        *out_val = make_int(matched ? 1 : 0);
        return SVDB_PURE_OK;
    }

    case SVDB_OP_MATCH: {
        /* MATCH: case-insensitive substring search */
        if (!v1 || !v2 || v1->val_type == SVDB_VAL_NULL || v2->val_type == SVDB_VAL_NULL) {
            *out_val = make_null(); return SVDB_PURE_OK;
        }
        char* tmp_str = nullptr;
        size_t tmp_len = 0;
        const char* h;
        size_t hlen;
        if (v1->val_type == SVDB_VAL_TEXT) {
            h    = v1->str_data ? v1->str_data : "";
            hlen = v1->str_len;
        } else {
            tmp_str = val_to_string(v1, &tmp_len);
            h    = tmp_str ? tmp_str : "";
            hlen = tmp_len;
        }
        size_t nlen = v2->str_len;
        if (nlen == 0) { if (tmp_str) free(tmp_str); *out_val = make_int(1); return SVDB_PURE_OK; }
        if (hlen < nlen) { if (tmp_str) free(tmp_str); *out_val = make_int(0); return SVDB_PURE_OK; }
        const char* n = (v2->str_data) ? v2->str_data : "";
        int found = 0;
        for (size_t i = 0; i + nlen <= hlen && !found; ++i) {
            found = 1;
            for (size_t j = 0; j < nlen && found; ++j) {
                if (ascii_lower((unsigned char)h[i+j]) != ascii_lower((unsigned char)n[j])) found = 0;
            }
        }
        if (tmp_str) free(tmp_str);
        *out_val = make_int(found ? 1 : 0);
        return SVDB_PURE_OK;
    }

    /* ── Math functions ───────────────────────────────────────────────── */
    case SVDB_OP_ABS:
        if (!v1 || v1->val_type == SVDB_VAL_NULL) { *out_val = make_null(); return SVDB_PURE_OK; }
        if (v1->val_type == SVDB_VAL_INT)
            *out_val = make_int(v1->int_val < 0 ? -v1->int_val : v1->int_val);
        else
            *out_val = make_float(fabs(val_to_double(v1)));
        return SVDB_PURE_OK;

    case SVDB_OP_ROUND: {
        if (!v1 || v1->val_type == SVDB_VAL_NULL) { *out_val = make_null(); return SVDB_PURE_OK; }
        double x = val_to_double(v1);
        int64_t decimals = aux_i;  /* passed via aux_i (Go extracts from reg[P2]) */
        if (decimals <= 0) {
            *out_val = make_float(round(x));
        } else {
            double scale = pow(10.0, (double)decimals);
            *out_val = make_float(round(x * scale) / scale);
        }
        return SVDB_PURE_OK;
    }

    case SVDB_OP_CEIL:
    case SVDB_OP_CEILING:
        if (!v1 || v1->val_type == SVDB_VAL_NULL) { *out_val = make_null(); return SVDB_PURE_OK; }
        *out_val = make_float(ceil(val_to_double(v1)));
        return SVDB_PURE_OK;

    case SVDB_OP_FLOOR:
        if (!v1 || v1->val_type == SVDB_VAL_NULL) { *out_val = make_null(); return SVDB_PURE_OK; }
        *out_val = make_float(floor(val_to_double(v1)));
        return SVDB_PURE_OK;

    case SVDB_OP_SQRT:
        if (!v1 || v1->val_type == SVDB_VAL_NULL) { *out_val = make_null(); return SVDB_PURE_OK; }
        *out_val = make_float(sqrt(val_to_double(v1)));
        return SVDB_PURE_OK;

    case SVDB_OP_POW:
        if (!v1 || !v2 || v1->val_type == SVDB_VAL_NULL || v2->val_type == SVDB_VAL_NULL) {
            *out_val = make_null(); return SVDB_PURE_OK;
        }
        *out_val = make_float(pow(val_to_double(v1), val_to_double(v2)));
        return SVDB_PURE_OK;

    case SVDB_OP_EXP:
        if (!v1 || v1->val_type == SVDB_VAL_NULL) { *out_val = make_null(); return SVDB_PURE_OK; }
        *out_val = make_float(exp(val_to_double(v1)));
        return SVDB_PURE_OK;

    case SVDB_OP_LOG:
    case SVDB_OP_LOG10:
        if (!v1 || v1->val_type == SVDB_VAL_NULL) { *out_val = make_null(); return SVDB_PURE_OK; }
        *out_val = make_float(log10(val_to_double(v1)));
        return SVDB_PURE_OK;

    case SVDB_OP_LN:
        if (!v1 || v1->val_type == SVDB_VAL_NULL) { *out_val = make_null(); return SVDB_PURE_OK; }
        *out_val = make_float(log(val_to_double(v1)));
        return SVDB_PURE_OK;

    case SVDB_OP_SIN:
        if (!v1 || v1->val_type == SVDB_VAL_NULL) { *out_val = make_null(); return SVDB_PURE_OK; }
        *out_val = make_float(sin(val_to_double(v1))); return SVDB_PURE_OK;
    case SVDB_OP_COS:
        if (!v1 || v1->val_type == SVDB_VAL_NULL) { *out_val = make_null(); return SVDB_PURE_OK; }
        *out_val = make_float(cos(val_to_double(v1))); return SVDB_PURE_OK;
    case SVDB_OP_TAN:
        if (!v1 || v1->val_type == SVDB_VAL_NULL) { *out_val = make_null(); return SVDB_PURE_OK; }
        *out_val = make_float(tan(val_to_double(v1))); return SVDB_PURE_OK;
    case SVDB_OP_ASIN:
        if (!v1 || v1->val_type == SVDB_VAL_NULL) { *out_val = make_null(); return SVDB_PURE_OK; }
        *out_val = make_float(asin(val_to_double(v1))); return SVDB_PURE_OK;
    case SVDB_OP_ACOS:
        if (!v1 || v1->val_type == SVDB_VAL_NULL) { *out_val = make_null(); return SVDB_PURE_OK; }
        *out_val = make_float(acos(val_to_double(v1))); return SVDB_PURE_OK;
    case SVDB_OP_ATAN:
        if (!v1 || v1->val_type == SVDB_VAL_NULL) { *out_val = make_null(); return SVDB_PURE_OK; }
        *out_val = make_float(atan(val_to_double(v1))); return SVDB_PURE_OK;
    case SVDB_OP_ATAN2:
        if (!v1 || !v2 || v1->val_type == SVDB_VAL_NULL || v2->val_type == SVDB_VAL_NULL) {
            *out_val = make_null(); return SVDB_PURE_OK;
        }
        *out_val = make_float(atan2(val_to_double(v1), val_to_double(v2)));
        return SVDB_PURE_OK;
    case SVDB_OP_SINH:
        if (!v1 || v1->val_type == SVDB_VAL_NULL) { *out_val = make_null(); return SVDB_PURE_OK; }
        *out_val = make_float(sinh(val_to_double(v1))); return SVDB_PURE_OK;
    case SVDB_OP_COSH:
        if (!v1 || v1->val_type == SVDB_VAL_NULL) { *out_val = make_null(); return SVDB_PURE_OK; }
        *out_val = make_float(cosh(val_to_double(v1))); return SVDB_PURE_OK;
    case SVDB_OP_TANH:
        if (!v1 || v1->val_type == SVDB_VAL_NULL) { *out_val = make_null(); return SVDB_PURE_OK; }
        *out_val = make_float(tanh(val_to_double(v1))); return SVDB_PURE_OK;

    case SVDB_OP_DEG_TO_RAD:
        if (!v1 || v1->val_type == SVDB_VAL_NULL) { *out_val = make_null(); return SVDB_PURE_OK; }
        *out_val = make_float(val_to_double(v1) * (3.14159265358979323846 / 180.0));
        return SVDB_PURE_OK;

    case SVDB_OP_RAD_TO_DEG:
        if (!v1 || v1->val_type == SVDB_VAL_NULL) { *out_val = make_null(); return SVDB_PURE_OK; }
        *out_val = make_float(val_to_double(v1) * (180.0 / 3.14159265358979323846));
        return SVDB_PURE_OK;

    /* ── Type conversion ──────────────────────────────────────────────── */
    case SVDB_OP_TO_TEXT: {
        if (!v1 || v1->val_type == SVDB_VAL_NULL) { *out_val = make_null(); return SVDB_PURE_OK; }
        if (v1->val_type == SVDB_VAL_TEXT) {
            *out_val = make_text(v1->str_data ? v1->str_data : "", v1->str_len);
        } else {
            size_t slen = 0;
            char* s = val_to_string(v1, &slen);
            *out_val = make_null();
            if (s) {
                out_val->val_type = SVDB_VAL_TEXT;
                out_val->str_data = s;
                out_val->str_len  = slen;
            }
        }
        return SVDB_PURE_OK;
    }

    case SVDB_OP_TO_INT:
    case SVDB_OP_REAL_TO_INT:
        if (!v1 || v1->val_type == SVDB_VAL_NULL) { *out_val = make_null(); return SVDB_PURE_OK; }
        *out_val = make_int(val_to_int64(v1));
        return SVDB_PURE_OK;

    case SVDB_OP_TO_REAL:
        if (!v1 || v1->val_type == SVDB_VAL_NULL) { *out_val = make_null(); return SVDB_PURE_OK; }
        *out_val = make_float(val_to_double(v1));
        return SVDB_PURE_OK;

    case SVDB_OP_TO_NUMERIC:
        if (!v1 || v1->val_type == SVDB_VAL_NULL) { *out_val = make_null(); return SVDB_PURE_OK; }
        if (v1->val_type == SVDB_VAL_INT || v1->val_type == SVDB_VAL_FLOAT) {
            *out_val = *v1;
        } else if (v1->val_type == SVDB_VAL_TEXT && v1->str_data && v1->str_len > 0) {
            /* Try int first, then float */
            char* end = nullptr;
            int64_t iv = strtoll(v1->str_data, &end, 10);
            if (end && *end == '\0') {
                *out_val = make_int(iv);
            } else {
                *out_val = make_float(strtod(v1->str_data, nullptr));
            }
        } else {
            *out_val = make_null();
        }
        return SVDB_PURE_OK;

    /* ── Misc ─────────────────────────────────────────────────────────── */
    case SVDB_OP_TYPEOF: {
        if (!v1) { *out_val = make_null(); return SVDB_PURE_OK; }
        const char* name = typeof_name(v1->val_type);
        *out_val = make_text(name, strlen(name));
        return SVDB_PURE_OK;
    }

    case SVDB_OP_RANDOM:
        /* Intentionally not handled here: the static LCG state is not thread-safe
         * under concurrent CGO calls.  The Go layer handles RANDOM via rand.Int63(). */
        return SVDB_PURE_NOT_PURE;

    default:
        return SVDB_PURE_NOT_PURE;
    }

    /* v3, aux_s, aux_len are reserved for future ternary/escape-parameter opcodes. */
    (void)v3;
    (void)aux_s;
    (void)aux_len;
    return SVDB_PURE_OK;
}

} /* extern "C" */
