#include "value.h"
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <climits>

namespace svdb {

// Private helper implementations
bool Value::ToFloat(const Value& v, double& out) {
    switch (v.type) {
        case ValueType::Int:
            out = static_cast<double>(v.int_val);
            return true;
        case ValueType::Float:
            out = v.float_val;
            return true;
        case ValueType::Bool:
            out = static_cast<double>(v.int_val);
            return true;
        default:
            return false;
    }
}

int Value::CmpInt(int64_t a, int64_t b) {
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
}

int Value::CmpString(const std::string& a, const std::string& b) {
    return a.compare(b);
}

int Value::CmpBytes(const std::string& a, const std::string& b) {
    size_t min_len = std::min(a.size(), b.size());
    for (size_t i = 0; i < min_len; ++i) {
        if (static_cast<unsigned char>(a[i]) < static_cast<unsigned char>(b[i])) return -1;
        if (static_cast<unsigned char>(a[i]) > static_cast<unsigned char>(b[i])) return 1;
    }
    if (a.size() < b.size()) return -1;
    if (a.size() > b.size()) return 1;
    return 0;
}

int Value::Compare(const Value& a, const Value& b) {
    // NULL ordering
    if (a.type == ValueType::Null && b.type == ValueType::Null) {
        return 0;
    }
    if (a.type == ValueType::Null) {
        return -1;
    }
    if (b.type == ValueType::Null) {
        return 1;
    }

    // Numeric coercion: Int <-> Float
    double af, bf;
    bool a_is_num = ToFloat(a, af);
    bool b_is_num = ToFloat(b, bf);
    
    if (a_is_num && b_is_num) {
        if (std::isnan(af) || std::isnan(bf)) {
            // Handle NaN case
            if (std::isnan(af) && std::isnan(bf)) return 0;
            if (std::isnan(af)) return -1;
            return 1;
        }
        if (af < bf) return -1;
        if (af > bf) return 1;
        return 0;
    }

    // Same non-numeric type
    if (a.type == b.type) {
        switch (a.type) {
            case ValueType::String:
                return CmpString(a.str_val, b.str_val);
            case ValueType::Bytes:
                return CmpBytes(a.bytes_val, b.bytes_val);
            case ValueType::Bool:
            case ValueType::Int:
                return CmpInt(a.int_val, b.int_val);
            default:
                break;
        }
    }

    // Fall back to type order
    return CmpInt(static_cast<int64_t>(a.type), static_cast<int64_t>(b.type));
}

// C-compatible API implementations
extern "C" {

void svdb_value_init_null(svdb_value_t* v) {
    v->val_type = static_cast<int32_t>(ValueType::Null);
    v->int_val = 0;
    v->float_val = 0.0;
    v->str_data = nullptr;
    v->str_len = 0;
    v->bytes_data = nullptr;
    v->bytes_len = 0;
}

void svdb_value_init_int(svdb_value_t* v, int64_t val) {
    v->val_type = static_cast<int32_t>(ValueType::Int);
    v->int_val = val;
    v->float_val = 0.0;
    v->str_data = nullptr;
    v->str_len = 0;
    v->bytes_data = nullptr;
    v->bytes_len = 0;
}

void svdb_value_init_float(svdb_value_t* v, double val) {
    v->val_type = static_cast<int32_t>(ValueType::Float);
    v->int_val = 0;
    v->float_val = val;
    v->str_data = nullptr;
    v->str_len = 0;
    v->bytes_data = nullptr;
    v->bytes_len = 0;
}

void svdb_value_init_string(svdb_value_t* v, const char* str, size_t len) {
    v->val_type = static_cast<int32_t>(ValueType::String);
    v->int_val = 0;
    v->float_val = 0.0;
    v->str_data = str;
    v->str_len = len;
    v->bytes_data = nullptr;
    v->bytes_len = 0;
}

void svdb_value_init_bool(svdb_value_t* v, int bool_val) {
    v->val_type = static_cast<int32_t>(ValueType::Bool);
    v->int_val = bool_val ? 1 : 0;
    v->float_val = 0.0;
    v->str_data = nullptr;
    v->str_len = 0;
    v->bytes_data = nullptr;
    v->bytes_len = 0;
}

void svdb_value_init_bytes(svdb_value_t* v, const char* data, size_t len) {
    v->val_type = static_cast<int32_t>(ValueType::Bytes);
    v->int_val = 0;
    v->float_val = 0.0;
    v->str_data = nullptr;
    v->str_len = 0;
    v->bytes_data = data;
    v->bytes_len = len;
}

int svdb_value_is_null(const svdb_value_t* v) {
    return v->val_type == static_cast<int32_t>(ValueType::Null) ? 1 : 0;
}

int svdb_value_equal(const svdb_value_t* a, const svdb_value_t* b) {
    if (a->val_type == static_cast<int32_t>(ValueType::Null) || 
        b->val_type == static_cast<int32_t>(ValueType::Null)) {
        return 0;  // NULL != NULL per SQL semantics
    }
    return svdb_value_compare(a, b) == 0 ? 1 : 0;
}

int svdb_value_compare(const svdb_value_t* a, const svdb_value_t* b) {
    // NULL ordering
    if (a->val_type == static_cast<int32_t>(ValueType::Null) && 
        b->val_type == static_cast<int32_t>(ValueType::Null)) {
        return 0;
    }
    if (a->val_type == static_cast<int32_t>(ValueType::Null)) {
        return -1;
    }
    if (b->val_type == static_cast<int32_t>(ValueType::Null)) {
        return 1;
    }

    // Numeric coercion helper
    auto to_float = [](const svdb_value_t* v, double& out) -> bool {
        switch (static_cast<ValueType>(v->val_type)) {
            case ValueType::Int:
                out = static_cast<double>(v->int_val);
                return true;
            case ValueType::Float:
                out = v->float_val;
                return true;
            case ValueType::Bool:
                out = static_cast<double>(v->int_val);
                return true;
            default:
                return false;
        }
    };

    // Numeric coercion: Int <-> Float
    double af, bf;
    bool a_is_num = to_float(a, af);
    bool b_is_num = to_float(b, bf);
    
    if (a_is_num && b_is_num) {
        if (std::isnan(af) || std::isnan(bf)) {
            if (std::isnan(af) && std::isnan(bf)) return 0;
            if (std::isnan(af)) return -1;
            return 1;
        }
        if (af < bf) return -1;
        if (af > bf) return 1;
        return 0;
    }

    // Same non-numeric type
    if (a->val_type == b->val_type) {
        switch (static_cast<ValueType>(a->val_type)) {
            case ValueType::String: {
                int cmp = strncmp(a->str_data, b->str_data, std::min(a->str_len, b->str_len));
                if (cmp != 0) return cmp < 0 ? -1 : 1;
                if (a->str_len < b->str_len) return -1;
                if (a->str_len > b->str_len) return 1;
                return 0;
            }
            case ValueType::Bytes: {
                size_t min_len = std::min(a->bytes_len, b->bytes_len);
                int cmp = memcmp(a->bytes_data, b->bytes_data, min_len);
                if (cmp != 0) return cmp < 0 ? -1 : 1;
                if (a->bytes_len < b->bytes_len) return -1;
                if (a->bytes_len > b->bytes_len) return 1;
                return 0;
            }
            case ValueType::Bool:
            case ValueType::Int:
                if (a->int_val < b->int_val) return -1;
                if (a->int_val > b->int_val) return 1;
                return 0;
            default:
                break;
        }
    }

    // Fall back to type order
    if (a->val_type < b->val_type) return -1;
    if (a->val_type > b->val_type) return 1;
    return 0;
}

void svdb_value_parse(svdb_value_t* v, const char* str, size_t len) {
    // Check for NULL
    if (len == 4 && strncmp(str, "NULL", 4) == 0) {
        svdb_value_init_null(v);
        return;
    }

    std::string s(str, len);
    
    // Try to parse as int64
    char* endptr;
    errno = 0;
    long long int_val = strtoll(str, &endptr, 10);
    if (errno == 0 && endptr == str + len) {
        svdb_value_init_int(v, static_cast<int64_t>(int_val));
        return;
    }

    // Try to parse as float64
    errno = 0;
    double float_val = strtod(str, &endptr);
    if (errno == 0 && endptr == str + len) {
        svdb_value_init_float(v, float_val);
        return;
    }

    // Fall back to string
    svdb_value_init_string(v, str, len);
}

} // extern "C"

} // namespace svdb
