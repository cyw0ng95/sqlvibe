/*
 * hash_join.cpp — Hash join wrapper exposing optimized implementation
 *
 * This file provides the SC-layer interface to the VM hash join implementation.
 * The actual optimized hash join is in src/core/VM/hash_join_optimized.cpp
 */
#include "svdb.h"
#include "svdb_types.h"
#include "../VM/hash_join.h"

extern "C" {

/* Re-export the VM hash join function for SC layer use */
svdb_join_result_t svdb_sc_hash_join_batch(
    const svdb_row_t* left_rows,
    size_t left_count,
    const svdb_row_t* right_rows,
    size_t right_count,
    size_t left_join_key_col,
    size_t right_join_key_col,
    size_t num_left_cols,
    size_t num_right_cols,
    int include_left_nulls,
    int include_right_nulls
) {
    return svdb_hash_join_batch(
        left_rows, left_count,
        right_rows, right_count,
        left_join_key_col, right_join_key_col,
        num_left_cols, num_right_cols,
        include_left_nulls, include_right_nulls
    );
}

void svdb_sc_free_join_result(svdb_join_result_t* result) {
    svdb_free_join_result(result);
}

int svdb_sc_hash_join_simd_level(void) {
    return svdb_hash_join_simd_level();
}

} /* extern "C" */

/*
 * C++ helper functions for converting between Row and svdb_row_t
 * These are used by query.cpp for multi-table hash joins
 */

#include <vector>
#include <string>
#include <cstring>

namespace svdb {
namespace sc {

/* Convert a vector of Row (map<string, SvdbVal>) to svdb_row_t array.
 * The col_order vector specifies which columns to extract and in what order.
 * Returns a vector of svdb_row_t that must be freed with free_svdb_rows().
 */
static std::vector<svdb_row_t> rows_to_svdb_rows(
    const std::vector<Row>& rows,
    const std::vector<std::string>& col_order
) {
    std::vector<svdb_row_t> result;
    result.reserve(rows.size());

    for (const auto& row : rows) {
        svdb_row_t svdb_row;
        svdb_row.num_columns = col_order.size();
        svdb_row.values = new char*[col_order.size()];
        svdb_row.value_lens = new size_t[col_order.size()];

        for (size_t c = 0; c < col_order.size(); ++c) {
            auto it = row.find(col_order[c]);
            if (it == row.end() || it->second.type == SVDB_TYPE_NULL) {
                svdb_row.values[c] = nullptr;
                svdb_row.value_lens[c] = 0;
            } else if (it->second.type == SVDB_TYPE_INT) {
                /* Convert int to string */
                std::string s = std::to_string(it->second.ival);
                svdb_row.values[c] = new char[s.size() + 1];
                std::memcpy(svdb_row.values[c], s.c_str(), s.size() + 1);
                svdb_row.value_lens[c] = s.size();
            } else if (it->second.type == SVDB_TYPE_REAL) {
                /* Convert double to string */
                char buf[64];
                snprintf(buf, sizeof(buf), "%.17g", it->second.rval);
                size_t len = std::strlen(buf);
                svdb_row.values[c] = new char[len + 1];
                std::memcpy(svdb_row.values[c], buf, len + 1);
                svdb_row.value_lens[c] = len;
            } else {
                /* TEXT or BLOB */
                const std::string& s = it->second.sval;
                svdb_row.values[c] = new char[s.size() + 1];
                std::memcpy(svdb_row.values[c], s.c_str(), s.size() + 1);
                svdb_row.value_lens[c] = s.size();
            }
        }
        result.push_back(svdb_row);
    }

    return result;
}

/* Free a vector of svdb_row_t */
static void free_svdb_rows(std::vector<svdb_row_t>& rows) {
    for (auto& row : rows) {
        if (row.values) {
            for (size_t c = 0; c < row.num_columns; ++c) {
                delete[] row.values[c];
            }
            delete[] row.values;
        }
        delete[] row.value_lens;
    }
    rows.clear();
}

/* Convert svdb_join_result_t back to vector of Row.
 * left_col_order and right_col_order specify column names for the result.
 * left_alias and right_alias are used to prefix column names.
 */
static std::vector<Row> join_result_to_rows(
    const svdb_join_result_t& result,
    const std::vector<std::string>& left_col_order,
    const std::vector<std::string>& right_col_order,
    const std::string& left_alias,
    const std::string& right_alias,
    const std::string& left_tname,
    const std::string& right_tname
) {
    std::vector<Row> rows;
    rows.reserve(result.num_rows);

    for (size_t i = 0; i < result.num_rows; ++i) {
        const svdb_row_t& svdb_row = result.rows[i];
        Row row;

        /* Extract left columns */
        for (size_t c = 0; c < left_col_order.size() && c < svdb_row.num_columns; ++c) {
            SvdbVal val;
            if (svdb_row.values[c] == nullptr || svdb_row.value_lens[c] == 0) {
                val.type = SVDB_TYPE_NULL;
            } else {
                val.type = SVDB_TYPE_TEXT;
                val.sval = std::string(svdb_row.values[c], svdb_row.value_lens[c]);
            }
            row[left_col_order[c]] = val;
            row[left_tname + "." + left_col_order[c]] = val;
            if (!left_alias.empty()) {
                row[left_alias + "." + left_col_order[c]] = val;
            }
        }

        /* Extract right columns */
        size_t right_offset = left_col_order.size();
        for (size_t c = 0; c < right_col_order.size() && (right_offset + c) < svdb_row.num_columns; ++c) {
            SvdbVal val;
            size_t idx = right_offset + c;
            if (svdb_row.values[idx] == nullptr || svdb_row.value_lens[idx] == 0) {
                val.type = SVDB_TYPE_NULL;
            } else {
                val.type = SVDB_TYPE_TEXT;
                val.sval = std::string(svdb_row.values[idx], svdb_row.value_lens[idx]);
            }
            row[right_tname + "." + right_col_order[c]] = val;
            row[right_alias + "." + right_col_order[c]] = val;
            /* Add unprefixed column if not already present */
            if (row.find(right_col_order[c]) == row.end()) {
                row[right_col_order[c]] = val;
            }
        }

        rows.push_back(row);
    }

    return rows;
}

/* Find column index in col_order vector */
static size_t find_col_index(const std::vector<std::string>& col_order, const std::string& col_name) {
    for (size_t i = 0; i < col_order.size(); ++i) {
        if (col_order[i] == col_name) return i;
    }
    return (size_t)-1; /* Not found */
}

/* Perform hash join on two sets of rows.
 * This is the main entry point for multi-table hash joins.
 *
 * Parameters:
 *   left_rows: Left table rows (already joined with previous tables)
 *   left_col_order: Column order for left rows
 *   right_rows: Right table rows (new table to join)
 *   right_col_order: Column order for right rows
 *   left_key_col: Join key column name in left table
 *   right_key_col: Join key column name in right table
 *   left_alias: Alias for left table
 *   right_alias: Alias for right table
 *   left_tname: Table name for left
 *   right_tname: Table name for right
 *   is_left_join: True for LEFT JOIN (include unmatched left rows)
 *   is_right_join: True for RIGHT JOIN (include unmatched right rows)
 *
 * Returns: Joined rows
 */
static std::vector<Row> hash_join_rows(
    const std::vector<Row>& left_rows,
    const std::vector<std::string>& left_col_order,
    const std::vector<Row>& right_rows,
    const std::vector<std::string>& right_col_order,
    const std::string& left_key_col,
    const std::string& right_key_col,
    const std::string& left_alias,
    const std::string& right_alias,
    const std::string& left_tname,
    const std::string& right_tname,
    bool is_left_join,
    bool is_right_join
) {
    /* Find key column indices */
    size_t left_key_idx = find_col_index(left_col_order, left_key_col);
    size_t right_key_idx = find_col_index(right_col_order, right_key_col);

    if (left_key_idx == (size_t)-1 || right_key_idx == (size_t)-1) {
        /* Key column not found, return empty result */
        return std::vector<Row>();
    }

    /* Convert rows to svdb_row_t format */
    std::vector<svdb_row_t> left_svdb = rows_to_svdb_rows(left_rows, left_col_order);
    std::vector<svdb_row_t> right_svdb = rows_to_svdb_rows(right_rows, right_col_order);

    /* Perform hash join */
    svdb_join_result_t result = svdb_hash_join_batch(
        left_svdb.data(), left_svdb.size(),
        right_svdb.data(), right_svdb.size(),
        left_key_idx, right_key_idx,
        left_col_order.size(), right_col_order.size(),
        is_left_join ? 1 : 0,
        is_right_join ? 1 : 0
    );

    /* Convert result back to Row format */
    std::vector<Row> joined = join_result_to_rows(
        result,
        left_col_order, right_col_order,
        left_alias, right_alias,
        left_tname, right_tname
    );

    /* Cleanup */
    svdb_free_join_result(&result);
    free_svdb_rows(left_svdb);
    free_svdb_rows(right_svdb);

    return joined;
}

} /* namespace sc */
} /* namespace svdb */