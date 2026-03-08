/* svdb_util.h — Shared helper utilities for the svdb module */
#pragma once
#include <string>
#include <cctype>
#include <algorithm>
#include <unordered_map>

/* Constant for the internal rowid column name */
#define SVDB_ROWID_COLUMN "_rowid_"

static inline std::string svdb_str_upper(std::string s) {
    for (auto &c : s) c = (char)toupper((unsigned char)c);
    return s;
}

static inline std::string svdb_str_trim(const std::string &s) {
    size_t a = 0, b = s.size();
    while (a < b && isspace((unsigned char)s[a])) ++a;
    while (b > a && isspace((unsigned char)s[b-1])) --b;
    return s.substr(a, b - a);
}

/* Check if identifier is quoted (case-sensitive) */
static inline bool is_quoted_identifier(const std::string &name) {
    if (name.size() < 2) return false;
    return (name.front() == '"' && name.back() == '"') ||
           (name.front() == '`' && name.back() == '`') ||
           (name.front() == '[' && name.back() == ']');
}

/* Case-insensitive table lookup for unquoted identifiers */
template<typename MapType>
static inline typename MapType::iterator find_table_case_insensitive(
    MapType &map, const std::string &tname) {
    /* Check exact match first */
    auto it = map.find(tname);
    if (it != map.end()) return it;
    
    /* For unquoted identifiers, do case-insensitive lookup */
    if (!is_quoted_identifier(tname)) {
        std::string tname_upper = svdb_str_upper(tname);
        for (auto &kv : map) {
            if (svdb_str_upper(kv.first) == tname_upper) {
                return map.find(kv.first);
            }
        }
    }
    return map.end();
}

/* Case-insensitive count for unquoted identifiers */
template<typename MapType>
static inline bool contains_table_case_insensitive(
    const MapType &map, const std::string &tname) {
    /* Check exact match first */
    if (map.count(tname)) return true;
    
    /* For unquoted identifiers, do case-insensitive lookup */
    if (!is_quoted_identifier(tname)) {
        std::string tname_upper = svdb_str_upper(tname);
        for (auto &kv : map) {
            if (svdb_str_upper(kv.first) == tname_upper) {
                return true;
            }
        }
    }
    return false;
}
