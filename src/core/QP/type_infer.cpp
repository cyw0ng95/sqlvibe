#include "type_infer.h"
#include <cstring>
#include <cctype>
#include <cstdlib>

/* Case-insensitive comparison of two strings of given lengths */
static int icase_eq(const char* a, size_t alen,
                    const char* b, size_t blen)
{
    if (alen != blen) return 0;
    return strncasecmp(a, b, alen) == 0;
}

extern "C" {

int svdb_type_infer_literal(const char* value_str, size_t value_len)
{
    if (!value_str || value_len == 0) return SVDB_TYPE_NULL;

    /* NULL keyword */
    if (icase_eq(value_str, value_len, "NULL", 4)) return SVDB_TYPE_NULL;

    bool has_dot   = false;
    bool all_digit = true;
    size_t start   = 0;

    /* Allow leading sign */
    if (value_len > 0 && (value_str[0] == '+' || value_str[0] == '-'))
        start = 1;

    if (start >= value_len) return SVDB_TYPE_TEXT;

    for (size_t i = start; i < value_len; ++i) {
        char c = value_str[i];
        if (c == '.') {
            has_dot = true;
        } else if (!isdigit((unsigned char)c)) {
            all_digit = false;
            break;
        }
    }

    if (!all_digit) return SVDB_TYPE_TEXT;
    return has_dot ? SVDB_TYPE_FLOAT : SVDB_TYPE_INT;
}

int svdb_type_promote(int a, int b)
{
    if (a == SVDB_TYPE_NULL) return b;
    if (b == SVDB_TYPE_NULL) return a;
    if (a == b) return a;
    /* INT + FLOAT → FLOAT */
    if ((a == SVDB_TYPE_INT && b == SVDB_TYPE_FLOAT) ||
        (a == SVDB_TYPE_FLOAT && b == SVDB_TYPE_INT))
        return SVDB_TYPE_FLOAT;
    return SVDB_TYPE_ANY;
}

int svdb_type_from_name(const char* type_str, size_t type_len)
{
    if (!type_str || type_len == 0) return SVDB_TYPE_ANY;

    if (icase_eq(type_str, type_len, "INTEGER",  7)) return SVDB_TYPE_INT;
    if (icase_eq(type_str, type_len, "INT",       3)) return SVDB_TYPE_INT;
    if (icase_eq(type_str, type_len, "BIGINT",    6)) return SVDB_TYPE_INT;
    if (icase_eq(type_str, type_len, "SMALLINT",  8)) return SVDB_TYPE_INT;
    if (icase_eq(type_str, type_len, "TEXT",      4)) return SVDB_TYPE_TEXT;
    if (icase_eq(type_str, type_len, "VARCHAR",   7)) return SVDB_TYPE_TEXT;
    if (icase_eq(type_str, type_len, "CHAR",      4)) return SVDB_TYPE_TEXT;
    if (icase_eq(type_str, type_len, "REAL",      4)) return SVDB_TYPE_FLOAT;
    if (icase_eq(type_str, type_len, "DOUBLE",    6)) return SVDB_TYPE_FLOAT;
    if (icase_eq(type_str, type_len, "FLOAT",     5)) return SVDB_TYPE_FLOAT;
    if (icase_eq(type_str, type_len, "NUMERIC",   7)) return SVDB_TYPE_FLOAT;
    if (icase_eq(type_str, type_len, "BLOB",      4)) return SVDB_TYPE_BLOB;
    if (icase_eq(type_str, type_len, "BOOLEAN",   7)) return SVDB_TYPE_BOOL;
    if (icase_eq(type_str, type_len, "BOOL",      4)) return SVDB_TYPE_BOOL;
    if (icase_eq(type_str, type_len, "NULL",      4)) return SVDB_TYPE_NULL;
    return SVDB_TYPE_ANY;
}

int svdb_type_get_func_return_type(const char* func_name, size_t func_name_len)
{
    if (!func_name || func_name_len == 0) return SVDB_TYPE_ANY;

    /* INT-returning functions */
    static const char* int_funcs[] = {
        "COUNT", "LENGTH", "INSTR", "UNICODE", "CHANGES",
        "LAST_INSERT_ROWID", "ROWID", nullptr
    };
    for (int i = 0; int_funcs[i]; ++i)
        if (icase_eq(func_name, func_name_len,
                     int_funcs[i], strlen(int_funcs[i])))
            return SVDB_TYPE_INT;

    /* FLOAT-returning functions */
    static const char* float_funcs[] = {
        "AVG", "ROUND", "ABS", "CEIL", "FLOOR",
        "SQRT", "POW", "LOG", "EXP", nullptr
    };
    for (int i = 0; float_funcs[i]; ++i)
        if (icase_eq(func_name, func_name_len,
                     float_funcs[i], strlen(float_funcs[i])))
            return SVDB_TYPE_FLOAT;

    /* TEXT-returning functions */
    static const char* text_funcs[] = {
        "UPPER", "LOWER", "TRIM", "LTRIM", "RTRIM",
        "SUBSTR", "SUBSTRING", "REPLACE", "PRINTF", "FORMAT",
        "HEX", "QUOTE", "GROUP_CONCAT", nullptr
    };
    for (int i = 0; text_funcs[i]; ++i)
        if (icase_eq(func_name, func_name_len,
                     text_funcs[i], strlen(text_funcs[i])))
            return SVDB_TYPE_TEXT;

    return SVDB_TYPE_ANY;
}

} /* extern "C" */
