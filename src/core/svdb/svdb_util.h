/* svdb_util.h — Shared helper utilities for the svdb module */
#pragma once
#include <string>
#include <cctype>
#include <algorithm>

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
