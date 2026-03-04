/* vtab_api.cpp — C API for Virtual Table Operations Implementation */
#include "vtab_api.h"
#include "vtab_registry.h"
#include <cstring>
#include <vector>
#include <string>

using namespace svdb;

/* ── Internal wrapper structures ─────────────────────────────── */

struct svdb_vtab_module_s {
    VTabModule* cpp_module;
    svdb_vtab_callbacks_t callbacks;
    void* user_data;
    bool is_c_module;
};

struct svdb_vtab_s {
    VTab* cpp_vtab;
    svdb_vtab_module_t* module;
    std::vector<std::string> column_cache;
    bool columns_cached;
};

struct svdb_vtab_cursor_s {
    VTabCursor* cpp_cursor;
    svdb_vtab_t* vtab;
};

/* ── Module registration ─────────────────────────────────────── */

extern "C" {

svdb_code_t svdb_vtab_register_module(const char* name, svdb_vtab_module_t* module) {
    if (!name || !module) {
        return SVDB_ERR;
    }
    return VTabRegistry::Instance().RegisterModule(name, module->cpp_module) == 0 ? SVDB_OK : SVDB_ERR;
}

svdb_vtab_module_t* svdb_vtab_get_module(const char* name) {
    if (!name) {
        return nullptr;
    }

    VTabModule* cpp_mod = VTabRegistry::Instance().GetModule(name);
    if (!cpp_mod) {
        return nullptr;
    }

    /* For C++ modules registered directly, we can't return a wrapper */
    /* This function is primarily for C modules */
    return nullptr;
}

int svdb_vtab_has_module(const char* name) {
    if (!name) {
        return 0;
    }
    return VTabRegistry::Instance().HasModule(name) ? 1 : 0;
}

int svdb_vtab_get_module_count(void) {
    return static_cast<int>(VTabRegistry::Instance().GetModuleNames().size());
}

svdb_code_t svdb_vtab_get_module_name(int index, char* buffer, size_t buffer_size) {
    if (!buffer || buffer_size == 0) {
        return SVDB_ERR;
    }

    auto names = VTabRegistry::Instance().GetModuleNames();
    if (index < 0 || static_cast<size_t>(index) >= names.size()) {
        return SVDB_NOTFOUND;
    }

    strncpy(buffer, names[index].c_str(), buffer_size - 1);
    buffer[buffer_size - 1] = '\0';
    return SVDB_OK;
}

/* ── Virtual table operations ────────────────────────────────── */

svdb_vtab_t* svdb_vtab_create(svdb_vtab_module_t* module,
                              const char** args, int arg_count) {
    if (!module || !module->cpp_module) {
        return nullptr;
    }
    
    std::vector<std::string> cpp_args;
    if (args && arg_count > 0) {
        for (int i = 0; i < arg_count; i++) {
            cpp_args.push_back(args[i] ? args[i] : "");
        }
    }
    
    VTab* cpp_vtab = module->cpp_module->Create(cpp_args);
    if (!cpp_vtab) {
        return nullptr;
    }
    
    svdb_vtab_t* vtab = new (std::nothrow) svdb_vtab_t();
    if (!vtab) {
        if (module->is_c_module) {
            /* C module cleanup */
        } else {
            delete cpp_vtab;
        }
        return nullptr;
    }
    
    vtab->cpp_vtab = cpp_vtab;
    vtab->module = module;
    vtab->columns_cached = false;
    
    return vtab;
}

svdb_vtab_t* svdb_vtab_connect(svdb_vtab_module_t* module,
                               const char** args, int arg_count) {
    if (!module || !module->cpp_module) {
        return nullptr;
    }
    
    std::vector<std::string> cpp_args;
    if (args && arg_count > 0) {
        for (int i = 0; i < arg_count; i++) {
            cpp_args.push_back(args[i] ? args[i] : "");
        }
    }
    
    VTab* cpp_vtab = module->cpp_module->Connect(cpp_args);
    if (!cpp_vtab) {
        return nullptr;
    }
    
    svdb_vtab_t* vtab = new (std::nothrow) svdb_vtab_t();
    if (!vtab) {
        if (module->is_c_module) {
            /* C module cleanup */
        } else {
            delete cpp_vtab;
        }
        return nullptr;
    }
    
    vtab->cpp_vtab = cpp_vtab;
    vtab->module = module;
    vtab->columns_cached = false;
    
    return vtab;
}

int svdb_vtab_column_count(svdb_vtab_t* vtab) {
    if (!vtab || !vtab->cpp_vtab) {
        return 0;
    }
    
    if (vtab->module->is_c_module) {
        return vtab->module->callbacks.column_count(vtab);
    }
    
    return static_cast<int>(vtab->cpp_vtab->Columns().size());
}

const char* svdb_vtab_column_name(svdb_vtab_t* vtab, int col) {
    if (!vtab || !vtab->cpp_vtab) {
        return nullptr;
    }
    
    if (vtab->module->is_c_module) {
        return vtab->module->callbacks.column_name(vtab, col);
    }
    
    /* Cache columns for C++ modules */
    if (!vtab->columns_cached) {
        vtab->column_cache = vtab->cpp_vtab->Columns();
        vtab->columns_cached = true;
    }
    
    if (col < 0 || col >= static_cast<int>(vtab->column_cache.size())) {
        return nullptr;
    }
    
    return vtab->column_cache[col].c_str();
}

svdb_vtab_cursor_t* svdb_vtab_cursor_open(svdb_vtab_t* vtab) {
    if (!vtab || !vtab->cpp_vtab) {
        return nullptr;
    }
    
    VTabCursor* cpp_cursor = nullptr;
    
    if (vtab->module->is_c_module) {
        cpp_cursor = reinterpret_cast<VTabCursor*>(
            vtab->module->callbacks.cursor_open(vtab));
    } else {
        cpp_cursor = vtab->cpp_vtab->Open();
    }
    
    if (!cpp_cursor) {
        return nullptr;
    }
    
    svdb_vtab_cursor_t* cursor = new (std::nothrow) svdb_vtab_cursor_t();
    if (!cursor) {
        if (vtab->module->is_c_module) {
            vtab->module->callbacks.cursor_close(
                reinterpret_cast<svdb_vtab_cursor_t*>(cpp_cursor));
        } else {
            delete cpp_cursor;
        }
        return nullptr;
    }
    
    cursor->cpp_cursor = cpp_cursor;
    cursor->vtab = vtab;
    
    return cursor;
}

svdb_code_t svdb_vtab_close(svdb_vtab_t* vtab, int destroy) {
    if (!vtab || !vtab->cpp_vtab) {
        return SVDB_ERR;
    }
    
    int result = 0;
    
    if (vtab->module->is_c_module) {
        /* C module: just delete wrapper */
    } else {
        if (destroy) {
            result = vtab->cpp_vtab->Destroy();
        } else {
            result = vtab->cpp_vtab->Disconnect();
        }
        delete vtab->cpp_vtab;
    }
    
    delete vtab;
    return result == 0 ? SVDB_OK : SVDB_ERR;
}

/* ── Cursor operations ───────────────────────────────────────── */

svdb_code_t svdb_vtab_cursor_filter(svdb_vtab_cursor_t* cursor, int idx_num,
                            const char* idx_str,
                            const char** args, int arg_count) {
    if (!cursor || !cursor->cpp_cursor) {
        return SVDB_ERR;
    }
    
    std::vector<std::string> cpp_args;
    if (args && arg_count > 0) {
        for (int i = 0; i < arg_count; i++) {
            cpp_args.push_back(args[i] ? args[i] : "");
        }
    }
    
    if (cursor->vtab->module->is_c_module) {
        return cursor->vtab->module->callbacks.cursor_filter(
            cursor, idx_num, idx_str,
            args ? args : nullptr, arg_count) == 0 ? SVDB_OK : SVDB_ERR;
    }
    
    return cursor->cpp_cursor->Filter(idx_num, idx_str ? idx_str : "", cpp_args) == 0 ? SVDB_OK : SVDB_ERR;
}

svdb_code_t svdb_vtab_cursor_next(svdb_vtab_cursor_t* cursor) {
    if (!cursor || !cursor->cpp_cursor) {
        return SVDB_ERR;
    }
    
    if (cursor->vtab->module->is_c_module) {
        return cursor->vtab->module->callbacks.cursor_next(cursor) == 0 ? SVDB_OK : SVDB_ERR;
    }
    
    return cursor->cpp_cursor->Next() == 0 ? SVDB_OK : SVDB_ERR;
}

int svdb_vtab_cursor_eof(svdb_vtab_cursor_t* cursor) {
    if (!cursor || !cursor->cpp_cursor) {
        return 1;
    }
    
    if (cursor->vtab->module->is_c_module) {
        return cursor->vtab->module->callbacks.cursor_eof(cursor);
    }
    
    return cursor->cpp_cursor->Eof() ? 1 : 0;
}

svdb_code_t svdb_vtab_cursor_column(svdb_vtab_cursor_t* cursor, int col,
                            int* out_type, int64_t* out_ival,
                            double* out_rval, const char** out_sval,
                            size_t* out_slen) {
    if (!cursor || !cursor->cpp_cursor) {
        return SVDB_ERR;
    }
    
    if (cursor->vtab->module->is_c_module) {
        return cursor->vtab->module->callbacks.cursor_column(
            cursor, col, out_type, out_ival, out_rval, out_sval, out_slen) == 0 ? SVDB_OK : SVDB_ERR;
    }
    
    return cursor->cpp_cursor->Column(col, out_type, out_ival, 
                                      out_rval, out_sval, out_slen) == 0 ? SVDB_OK : SVDB_ERR;
}

svdb_code_t svdb_vtab_cursor_rowid(svdb_vtab_cursor_t* cursor, int64_t* out_rowid) {
    if (!cursor || !cursor->cpp_cursor) {
        return SVDB_ERR;
    }
    
    if (cursor->vtab->module->is_c_module) {
        return cursor->vtab->module->callbacks.cursor_rowid(cursor, out_rowid) == 0 ? SVDB_OK : SVDB_ERR;
    }
    
    return cursor->cpp_cursor->RowID(out_rowid) == 0 ? SVDB_OK : SVDB_ERR;
}

svdb_code_t svdb_vtab_cursor_close(svdb_vtab_cursor_t* cursor) {
    if (!cursor) {
        return SVDB_ERR;
    }
    
    int result = 0;
    
    if (cursor->vtab->module->is_c_module) {
        result = cursor->vtab->module->callbacks.cursor_close(cursor);
    } else {
        result = cursor->cpp_cursor->Close();
        delete cursor->cpp_cursor;
    }
    
    delete cursor;
    return result == 0 ? SVDB_OK : SVDB_ERR;
}

/* ── C module interface ──────────────────────────────────────── */

/* Wrapper module class for C callbacks */
class CVTabModule : public VTabModule {
public:
    CVTabModule(const svdb_vtab_callbacks_t& cb, void* user_data)
        : callbacks_(cb), user_data_(user_data) {}
    
    VTab* Create(const std::vector<std::string>& args) override {
        std::vector<const char*> c_args;
        for (const auto& arg : args) {
            c_args.push_back(arg.c_str());
        }
        return reinterpret_cast<VTab*>(
            callbacks_.create(c_args.data(), static_cast<int>(c_args.size())));
    }
    
    VTab* Connect(const std::vector<std::string>& args) override {
        std::vector<const char*> c_args;
        for (const auto& arg : args) {
            c_args.push_back(arg.c_str());
        }
        return reinterpret_cast<VTab*>(
            callbacks_.connect(c_args.data(), static_cast<int>(c_args.size())));
    }

private:
    svdb_vtab_callbacks_t callbacks_;
    void* user_data_;
};

/* Wrapper VTab class for C callbacks */
class CVTab : public VTab {
public:
    CVTab(svdb_vtab_t* wrapper, const svdb_vtab_callbacks_t& cb)
        : wrapper_(wrapper), callbacks_(cb) {}
    
    std::vector<std::string> Columns() const override {
        std::vector<std::string> cols;
        int count = callbacks_.column_count(wrapper_);
        for (int i = 0; i < count; i++) {
            const char* name = callbacks_.column_name(wrapper_, i);
            if (name) {
                cols.push_back(name);
            }
        }
        return cols;
    }
    
    VTabCursor* Open() override {
        return reinterpret_cast<VTabCursor*>(
            callbacks_.cursor_open(wrapper_));
    }

private:
    svdb_vtab_t* wrapper_;
    svdb_vtab_callbacks_t callbacks_;
};

/* Wrapper cursor class for C callbacks */
class CVTabCursor : public VTabCursor {
public:
    CVTabCursor(svdb_vtab_cursor_t* wrapper, 
                const svdb_vtab_callbacks_t& cb)
        : wrapper_(wrapper), callbacks_(cb) {}
    
    int Filter(int idxNum, const std::string& idxStr,
               const std::vector<std::string>& args) override {
        std::vector<const char*> c_args;
        for (const auto& arg : args) {
            c_args.push_back(arg.c_str());
        }
        return callbacks_.cursor_filter(
            wrapper_, idxNum, idxStr.c_str(),
            c_args.data(), static_cast<int>(c_args.size()));
    }
    
    int Next() override {
        return callbacks_.cursor_next(wrapper_);
    }
    
    bool Eof() const override {
        return callbacks_.cursor_eof(wrapper_) != 0;
    }
    
    int Column(int col, int* out_type, int64_t* out_ival,
               double* out_rval, const char** out_sval,
               size_t* out_slen) override {
        return callbacks_.cursor_column(
            wrapper_, col, out_type, out_ival, out_rval, out_sval, out_slen);
    }
    
    int RowID(int64_t* out_rowid) override {
        return callbacks_.cursor_rowid(wrapper_, out_rowid);
    }
    
    int Close() override {
        return callbacks_.cursor_close(wrapper_);
    }

private:
    svdb_vtab_cursor_t* wrapper_;
    svdb_vtab_callbacks_t callbacks_;
};

svdb_vtab_module_t* svdb_vtab_module_create_c(
    const svdb_vtab_callbacks_t* callbacks, void* user_data) {
    if (!callbacks) {
        return nullptr;
    }
    
    svdb_vtab_module_t* module = new (std::nothrow) svdb_vtab_module_t();
    if (!module) {
        return nullptr;
    }
    
    module->cpp_module = new (std::nothrow) CVTabModule(*callbacks, user_data);
    if (!module->cpp_module) {
        delete module;
        return nullptr;
    }
    
    module->callbacks = *callbacks;
    module->user_data = user_data;
    module->is_c_module = true;
    
    return module;
}

void svdb_vtab_module_set_user_data(svdb_vtab_module_t* module,
                                    void* user_data) {
    if (module) {
        module->user_data = user_data;
    }
}

void* svdb_vtab_module_get_user_data(svdb_vtab_module_t* module) {
    return module ? module->user_data : nullptr;
}

} /* extern "C" */
