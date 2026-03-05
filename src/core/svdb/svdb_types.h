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
    bool        not_null   = false;
    bool        primary_key = false;
    bool        auto_increment = false; /* INTEGER PRIMARY KEY AUTOINCREMENT */
};

/* Table-level check constraint expression */
using CheckList = std::vector<std::string>;

/* Foreign key definition: child col -> (parent table, parent col) */
struct FKDef {
    std::string child_col;
    std::string parent_table;
    std::string parent_col;
    std::string on_delete; /* "CASCADE", "SET NULL", "RESTRICT", or "" */
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
    /* Unique column sets per table (each entry = set of column names) */
    std::unordered_map<std::string, std::vector<std::vector<std::string>>> unique_constraints;
    /* CHECK constraints per table */
    std::unordered_map<std::string, CheckList>                         check_constraints;
    /* Foreign key constraints per table */
    std::unordered_map<std::string, std::vector<FKDef>>                fk_constraints;
    /* CREATE TABLE original SQL for each table/view */
    std::unordered_map<std::string, std::string>                       create_sql;

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

    /* PRAGMA settings */
    std::string wal_mode         = "OFF";
    std::string isolation_level  = "READ COMMITTED";
    int64_t     busy_timeout_ms  = 0;
    std::string compression      = "NONE";
    bool        foreign_keys_enabled = false;
    int64_t     max_rows         = 0;       /* 0 = unlimited */
    int64_t     cache_memory     = 2097152; /* 2 MB default */
    std::string synchronous      = "NORMAL";
    int64_t     query_timeout_ms = 0;       /* 0 = no timeout */
    int64_t     max_memory       = 0;       /* 0 = unlimited */

    /* Transaction state */
    bool         in_transaction = false;
    svdb_tx_t   *sql_tx         = nullptr;  /* active SQL-level transaction */

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

    /* Snapshot for rollback: table data at BEGIN time */
    std::unordered_map<std::string, std::vector<Row>> data_snapshot;
    std::unordered_map<std::string, int64_t>          rowid_snapshot;
    /* Savepoint stacks */
    std::vector<std::unordered_map<std::string, std::vector<Row>>> sp_data;
    std::vector<std::unordered_map<std::string, int64_t>>          sp_rowid;
};
