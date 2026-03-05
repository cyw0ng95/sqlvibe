#include "bytecode_compiler.h"
#include <cstring>
#include <cctype>

/* Case-insensitive substring search within a bounded buffer */
static int icase_find(const char* sql, size_t sql_len, const char* needle)
{
    if (!sql || !needle) return 0;
    size_t nl = strlen(needle);
    if (nl == 0 || nl > sql_len) return 0;
    for (size_t i = 0; i <= sql_len - nl; ++i) {
        bool ok = true;
        for (size_t j = 0; j < nl; ++j) {
            if (tolower((unsigned char)sql[i + j]) !=
                tolower((unsigned char)needle[j])) {
                ok = false; break;
            }
        }
        if (ok) return 1;
    }
    return 0;
}

extern "C" {

int svdb_bc_can_use_fast_path(const char* sql, size_t sql_len)
{
    if (!sql || sql_len == 0) return 0;

    static const char* blockers[] = {
        "WITH", "WINDOW", "JOIN", "UNION", "INTERSECT", "EXCEPT", "OVER",
        nullptr
    };
    for (int i = 0; blockers[i]; ++i)
        if (icase_find(sql, sql_len, blockers[i])) return 0;

    /* Must start with SELECT */
    size_t pos = 0;
    while (pos < sql_len && isspace((unsigned char)sql[pos])) ++pos;
    if (sql_len - pos < 6) return 0;
    char sel[7] = {};
    for (int k = 0; k < 6; ++k)
        sel[k] = (char)tolower((unsigned char)sql[pos + k]);
    return strncmp(sel, "select", 6) == 0 ? 1 : 0;
}

int svdb_bc_has_aggregates(const char* sql, size_t sql_len)
{
    if (!sql || sql_len == 0) return 0;
    static const char* aggs[] = {
        "SUM(", "COUNT(", "AVG(", "MIN(", "MAX(", "GROUP_CONCAT(",
        nullptr
    };
    for (int i = 0; aggs[i]; ++i)
        if (icase_find(sql, sql_len, aggs[i])) return 1;
    return 0;
}

int svdb_bc_needs_sort(const char* sql, size_t sql_len)
{
    return icase_find(sql, sql_len, "ORDER BY");
}

int svdb_bc_has_limit(const char* sql, size_t sql_len)
{
    return icase_find(sql, sql_len, "LIMIT");
}

int svdb_bc_has_group_by(const char* sql, size_t sql_len)
{
    return icase_find(sql, sql_len, "GROUP BY");
}

int svdb_bc_has_window_func(const char* sql, size_t sql_len)
{
    return icase_find(sql, sql_len, "OVER (");
}

int svdb_bc_estimate_reg_count(int num_columns, int has_where, int has_agg)
{
    if (num_columns < 0) num_columns = 0;
    return num_columns
         + (has_where ? 2 : 0)
         + (has_agg   ? num_columns : 0)
         + 4;
}

} /* extern "C" */
