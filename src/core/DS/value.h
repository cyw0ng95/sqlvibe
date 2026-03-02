#ifndef SVDB_VALUE_H
#define SVDB_VALUE_H

#include <stdint.h>
#include <stddef.h>
#include "../SF/types.h"

#ifdef __cplusplus
#include <cstdint>
#include <cstring>
#include <string>
#include <cmath>

namespace svdb {

// ValueType enumerates the supported column types.
enum class ValueType : int32_t {
    Null = 0,
    Int = 1,       // int64
    Float = 2,     // float64
    String = 3,    // string
    Bytes = 4,     // []byte
    Bool = 5       // bool
};

// Value holds a single typed datum.
struct Value {
    ValueType type;
    int64_t int_val;
    double float_val;
    std::string str_val;
    std::string bytes_val;  // Use string for byte array storage

    Value() : type(ValueType::Null), int_val(0), float_val(0.0) {}

    static Value Null() {
        Value v;
        v.type = ValueType::Null;
        return v;
    }

    static Value Int(int64_t v) {
        Value val;
        val.type = ValueType::Int;
        val.int_val = v;
        return val;
    }

    static Value Float(double v) {
        Value val;
        val.type = ValueType::Float;
        val.float_val = v;
        return val;
    }

    static Value String(const std::string& v) {
        Value val;
        val.type = ValueType::String;
        val.str_val = v;
        return val;
    }

    static Value Bool(bool v) {
        Value val;
        val.type = ValueType::Bool;
        val.int_val = v ? 1 : 0;
        return val;
    }

    static Value Bytes(const char* data, size_t len) {
        Value val;
        val.type = ValueType::Bytes;
        val.bytes_val.assign(data, len);
        return val;
    }

    bool IsNull() const { return type == ValueType::Null; }

    std::string ToString() const;
    bool Equal(const Value& other) const;
    static int Compare(const Value& a, const Value& b);

private:
    static bool ToFloat(const Value& v, double& out);
    static int CmpInt(int64_t a, int64_t b);
    static int CmpString(const std::string& a, const std::string& b);
    static int CmpBytes(const std::string& a, const std::string& b);
};

} // namespace svdb
#endif // __cplusplus

/* C-compatible API for CGO (visible to both C and C++) */
/* svdb_value_t is defined in ../SF/types.h */

#ifdef __cplusplus
extern "C" {
#endif

void svdb_value_init_null(svdb_value_t* v);
void svdb_value_init_int(svdb_value_t* v, int64_t val);
void svdb_value_init_float(svdb_value_t* v, double val);
void svdb_value_init_string(svdb_value_t* v, const char* str, size_t len);
void svdb_value_init_bool(svdb_value_t* v, int bool_val);
void svdb_value_init_bytes(svdb_value_t* v, const char* data, size_t len);

int svdb_value_is_null(const svdb_value_t* v);
int svdb_value_equal(const svdb_value_t* a, const svdb_value_t* b);
int svdb_value_compare(const svdb_value_t* a, const svdb_value_t* b);

// Parse a string to Value
void svdb_value_parse(svdb_value_t* v, const char* str, size_t len);

#ifdef __cplusplus
} // extern "C"
#endif

#endif // SVDB_VALUE_H
