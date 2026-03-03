#pragma once
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <mutex>
#include "svdb.h"

/* Column type string e.g. "INTEGER", "TEXT", "REAL", "BLOB" */
using ColType = std::string;

/* Per-column definition: type + default + not-null flag */
struct ColDef {
    ColType     type;
    std::string default_val;
    bool        not_null = false;
};

/* Table schema: column name -> ColDef */
using TableDef = std::unordered_map<std::string, ColDef>;

/* A single cell value stored in memory */
struct SvdbVal {
    svdb_type_t type   = SVDB_TYPE_NULL;
    int64_t     ival   = 0;
    double      rval   = 0.0;
    std::string sval;   /* TEXT or BLOB */
};

/* A row: column name -> value */
using Row = std::unordered_map<std::string, SvdbVal>;

/* Index definition */
struct IndexDef {
    std::string table;
    std::vector<std::string> columns;
    bool unique = false;
};

/* Database state */
struct svdb_db_s {
    std::string path;

    /* Schema metadata */
    std::unordered_map<std::string, TableDef>                          schema;
    std::unordered_map<std::string, std::vector<std::string>>          primary_keys;
    std::unordered_map<std::string, std::vector<std::string>>          col_order;

    /* In-memory row storage: table_name -> rows */
    std::unordered_map<std::string, std::vector<Row>>                  data;

    /* Index metadata: index_name -> IndexDef */
    std::map<std::string, IndexDef>                                    indexes;

    /* Auto-increment counters: table_name -> last rowid */
    std::unordered_map<std::string, int64_t>                           rowid_counter;

    /* Last DML stats */
    int64_t  rows_affected      = 0;
    int64_t  last_insert_rowid  = 0;

    /* Last error */
    std::string last_error;

    /* Thread safety */
    std::mutex mu;
};

/* Result set */
struct svdb_rows_s {
    std::vector<std::string>  col_names;
    std::vector<std::vector<SvdbVal>> rows;
    int cursor = -1;   /* points at current row; -1 = before first */

    /* String storage for svdb_rows_get() sval pointers */
    std::vector<std::string> str_store;
};

/* Prepared statement */
struct svdb_stmt_s {
    svdb_db_t  *db = nullptr;
    std::string sql;
    std::map<int, SvdbVal> bindings;  /* idx (1-based) -> value */
};

/* Transaction */
struct svdb_tx_s {
    svdb_db_t               *db          = nullptr;
    bool                     committed   = false;
    std::vector<std::string> savepoints;
};
