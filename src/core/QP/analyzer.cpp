#include "analyzer.h"
#include <cstring>
#include <cctype>
#include <cstdio>
#include <string>
#include <vector>
#include <unordered_set>

/* ------------------------------------------------------------------ helpers */

static bool icase_contains(const char* haystack, const char* needle)
{
    if (!haystack || !needle) return false;
    size_t hl = strlen(haystack);
    size_t nl = strlen(needle);
    if (nl == 0 || nl > hl) return false;
    for (size_t i = 0; i <= hl - nl; ++i) {
        bool match = true;
        for (size_t j = 0; j < nl; ++j) {
            if (tolower((unsigned char)haystack[i + j]) !=
                tolower((unsigned char)needle[j])) {
                match = false;
                break;
            }
        }
        if (match) return true;
    }
    return false;
}

/* Extract identifier-like tokens (alphanumeric + underscore) from sql */
static void extract_identifiers(const char* sql,
                                 std::unordered_set<std::string>& out)
{
    if (!sql) return;
    const char* p = sql;
    while (*p) {
        if (isalpha((unsigned char)*p) || *p == '_') {
            const char* start = p;
            while (isalnum((unsigned char)*p) || *p == '_') ++p;
            out.insert(std::string(start, p));
        } else {
            ++p;
        }
    }
}

/* Split a comma-separated string into tokens */
static void split_csv(const char* csv,
                      std::unordered_set<std::string>& out)
{
    if (!csv) return;
    const char* p = csv;
    while (*p) {
        while (*p == ' ' || *p == '\t') ++p;
        if (!*p) break;
        const char* start = p;
        while (*p && *p != ',') ++p;
        const char* end = p;
        while (end > start && (*(end - 1) == ' ' || *(end - 1) == '\t'))
            --end;
        if (end > start)
            out.insert(std::string(start, end));
        if (*p == ',') ++p;
    }
}

/* ------------------------------------------------------------------ API */

extern "C" {

int svdb_analyzer_required_columns(
    const char* column_names_csv,
    const char* where_sql,
    const char* orderby_sql,
    const char* groupby_sql,
    char*       out_buf,
    int         out_buf_size)
{
    std::unordered_set<std::string> cols;
    split_csv(column_names_csv, cols);
    extract_identifiers(where_sql, cols);
    extract_identifiers(orderby_sql, cols);
    extract_identifiers(groupby_sql, cols);

    /* Build CSV result */
    std::string result;
    for (const auto& c : cols) {
        if (!result.empty()) result += ',';
        result += c;
    }

    int need = (int)result.size() + 1;
    if (out_buf_size < need) return -1;
    memcpy(out_buf, result.c_str(), (size_t)need);
    return (int)cols.size();
}

int svdb_analyzer_has_aggregates(const char* sql)
{
    if (!sql) return 0;
    static const char* aggs[] = {"SUM(", "COUNT(", "AVG(", "MIN(", "MAX(", nullptr};
    for (int i = 0; aggs[i]; ++i)
        if (icase_contains(sql, aggs[i])) return 1;
    return 0;
}

int svdb_analyzer_has_subquery(const char* sql)
{
    if (!sql) return 0;
    /* Look for SELECT inside parentheses */
    const char* p = sql;
    int depth = 0;
    while (*p) {
        if (*p == '(') {
            ++depth;
            if (depth > 0) {
                /* peek ahead for SELECT */
                const char* q = p + 1;
                while (*q == ' ' || *q == '\t' || *q == '\n' || *q == '\r') ++q;
                if (strncasecmp(q, "SELECT", 6) == 0) return 1;
            }
        } else if (*p == ')') {
            --depth;
        }
        ++p;
    }
    return 0;
}

int svdb_analyzer_is_star_select(const char* column_list_sql)
{
    if (!column_list_sql) return 0;
    /* Skip leading whitespace */
    const char* p = column_list_sql;
    while (*p == ' ' || *p == '\t') ++p;
    if (strcmp(p, "*") == 0) return 1;
    /* Check for t.* pattern */
    const char* dot = strrchr(p, '.');
    if (dot && strcmp(dot + 1, "*") == 0) return 1;
    return 0;
}

int svdb_analyzer_count_join_tables(const char* from_sql)
{
    if (!from_sql || from_sql[0] == '\0') return 0;
    int count = 1;
    int paren = 0;
    for (const char* p = from_sql; *p; ++p) {
        if (*p == '(') ++paren;
        else if (*p == ')') --paren;
        else if (*p == ',' && paren == 0) ++count;
    }
    return count;
}

} /* extern "C" */
