// BenchmarkCpp - C++ Benchmark using Google Benchmark
// Compares sqlvibe (svdb.h C API) vs SQLite3 performance

#include <benchmark/benchmark.h>
#include <svdb.h>
#include <sqlite3.h>
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <vector>
#include <string>
#include <random>

// Number of rows per benchmark
static const int kNumRows = 1000;

// Random number generator for reproducible data
static std::mt19937_64 g_rng(42);

// ============================================================================
// Helper Functions
// ============================================================================

static int64_t GenerateRandomInt(int64_t min_val, int64_t max_val) {
    std::uniform_int_distribution<int64_t> dist(min_val, max_val);
    return dist(g_rng);
}

static double GenerateRandomReal() {
    std::uniform_real_distribution<double> dist(0.0, 10000.0);
    return dist(g_rng);
}

static std::string GenerateRandomString(int len) {
    const char chars[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::uniform_int_distribution<int> dist(0, sizeof(chars) - 2);
    std::string result;
    result.reserve(len);
    for (int i = 0; i < len; ++i) {
        result += chars[dist(g_rng)];
    }
    return result;
}

// ============================================================================
// SQLite Helper Functions
// ============================================================================

static void CreateSQLiteTable(sqlite3* db, const char* table_name) {
    std::string sql = "DROP TABLE IF EXISTS " + std::string(table_name) + ";";
    sqlite3_exec(db, sql.c_str(), nullptr, nullptr, nullptr);

    sql = "CREATE TABLE " + std::string(table_name) +
          "(id INTEGER PRIMARY KEY, x INTEGER, y REAL, z TEXT);";
    sqlite3_exec(db, sql.c_str(), nullptr, nullptr, nullptr);
}

static void InsertDataSQLite(sqlite3* db, const char* table_name, int num_rows) {
    sqlite3_exec(db, "BEGIN TRANSACTION", nullptr, nullptr, nullptr);

    std::string sql = "INSERT INTO " + std::string(table_name) + " VALUES (?, ?, ?, ?);";
    sqlite3_stmt* stmt;
    sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr);

    for (int i = 1; i <= num_rows; ++i) {
        sqlite3_bind_int64(stmt, 1, i);
        sqlite3_bind_int64(stmt, 2, GenerateRandomInt(1, 10000));
        sqlite3_bind_double(stmt, 3, GenerateRandomReal());
        std::string s = GenerateRandomString(10);
        sqlite3_bind_text(stmt, 4, s.c_str(), s.length(), SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_reset(stmt);
    }

    sqlite3_finalize(stmt);
    sqlite3_exec(db, "COMMIT", nullptr, nullptr, nullptr);
}

// ============================================================================
// SVDB Helper Functions
// ============================================================================

static void CreateSVDBTable(svdb_db_t* db, const char* table_name) {
    std::string sql = "DROP TABLE IF EXISTS " + std::string(table_name) + ";";
    svdb_exec(db, sql.c_str(), nullptr);

    sql = "CREATE TABLE " + std::string(table_name) +
          "(id INTEGER PRIMARY KEY, x INTEGER, y REAL, z TEXT);";
    svdb_exec(db, sql.c_str(), nullptr);
}

static void InsertDataSVDB(svdb_db_t* db, const char* table_name, int num_rows) {
    svdb_tx_t* tx = nullptr;
    svdb_begin(db, &tx);

    std::string sql = "INSERT INTO " + std::string(table_name) + " VALUES (?, ?, ?, ?);";
    svdb_stmt_t* stmt = nullptr;
    svdb_prepare(db, sql.c_str(), &stmt);

    for (int i = 1; i <= num_rows; ++i) {
        svdb_stmt_bind_int(stmt, 1, i);
        svdb_stmt_bind_int(stmt, 2, GenerateRandomInt(1, 10000));
        svdb_stmt_bind_real(stmt, 3, GenerateRandomReal());
        std::string s = GenerateRandomString(10);
        svdb_stmt_bind_text(stmt, 4, s.c_str(), s.length());
        svdb_stmt_exec(stmt, nullptr);
        svdb_stmt_reset(stmt);
    }

    svdb_stmt_close(stmt);
    svdb_commit(tx);
}

// ============================================================================
// Benchmarks - Insert
// ============================================================================

static void BM_InsertSingle_SVDB(benchmark::State& state) {
    for (auto _ : state) {
        svdb_db_t* db = nullptr;
        svdb_open(":memory:", &db);

        CreateSVDBTable(db, "t");

        svdb_stmt_t* stmt = nullptr;
        svdb_prepare(db, "INSERT INTO t VALUES (?, ?, ?, ?);", &stmt);

        for (int i = 1; i <= kNumRows; ++i) {
            svdb_stmt_bind_int(stmt, 1, i);
            svdb_stmt_bind_int(stmt, 2, i);
            svdb_stmt_bind_real(stmt, 3, (double)i);
            char text[32];
            snprintf(text, sizeof(text), "text%d", i);
            svdb_stmt_bind_text(stmt, 4, text, strlen(text));
            svdb_stmt_exec(stmt, nullptr);
            svdb_stmt_reset(stmt);
        }

        svdb_stmt_close(stmt);
        svdb_close(db);
    }
}
BENCHMARK(BM_InsertSingle_SVDB);

static void BM_InsertSingle_SQLite(benchmark::State& state) {
    for (auto _ : state) {
        sqlite3* db = nullptr;
        sqlite3_open(":memory:", &db);

        CreateSQLiteTable(db, "t");

        for (int i = 1; i <= kNumRows; ++i) {
            char sql[256];
            snprintf(sql, sizeof(sql), "INSERT INTO t VALUES (%d, %d, %.6f, 'text%d');",
                     i, i, (double)i, i);
            sqlite3_exec(db, sql, nullptr, nullptr, nullptr);
        }

        sqlite3_close(db);
    }
}
BENCHMARK(BM_InsertSingle_SQLite);

// ============================================================================
// Benchmarks - Batch Insert
// ============================================================================

static void BM_InsertBatch_SVDB(benchmark::State& state) {
    for (auto _ : state) {
        svdb_db_t* db = nullptr;
        svdb_open(":memory:", &db);

        CreateSVDBTable(db, "t");
        InsertDataSVDB(db, "t", kNumRows);

        svdb_close(db);
    }
}
BENCHMARK(BM_InsertBatch_SVDB);

static void BM_InsertBatch_SQLite(benchmark::State& state) {
    for (auto _ : state) {
        sqlite3* db = nullptr;
        sqlite3_open(":memory:", &db);

        CreateSQLiteTable(db, "t");
        InsertDataSQLite(db, "t", kNumRows);

        sqlite3_close(db);
    }
}
BENCHMARK(BM_InsertBatch_SQLite);

// ============================================================================
// Benchmarks - Select All
// ============================================================================

static void BM_SelectAll_SVDB(benchmark::State& state) {
    for (auto _ : state) {
        svdb_db_t* db = nullptr;
        svdb_open(":memory:", &db);

        CreateSVDBTable(db, "t");
        InsertDataSVDB(db, "t", kNumRows);

        svdb_rows_t* rows = nullptr;
        svdb_query(db, "SELECT * FROM t", &rows);

        int count = 0;
        while (svdb_rows_next(rows)) {
            svdb_val_t v0 = svdb_rows_get(rows, 0);
            svdb_val_t v1 = svdb_rows_get(rows, 1);
            svdb_val_t v2 = svdb_rows_get(rows, 2);
            svdb_val_t v3 = svdb_rows_get(rows, 3);
            (void)v0; (void)v1; (void)v2; (void)v3;
            count++;
        }
        svdb_rows_close(rows);

        svdb_close(db);
    }
}
BENCHMARK(BM_SelectAll_SVDB);

static void BM_SelectAll_SQLite(benchmark::State& state) {
    for (auto _ : state) {
        sqlite3* db = nullptr;
        sqlite3_open(":memory:", &db);

        CreateSQLiteTable(db, "t");
        InsertDataSQLite(db, "t", kNumRows);

        sqlite3_stmt* stmt = nullptr;
        sqlite3_prepare_v2(db, "SELECT * FROM t", -1, &stmt, nullptr);

        int count = 0;
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            sqlite3_column_int64(stmt, 0);
            sqlite3_column_int64(stmt, 1);
            sqlite3_column_double(stmt, 2);
            sqlite3_column_text(stmt, 3);
            count++;
        }
        sqlite3_finalize(stmt);

        sqlite3_close(db);
    }
}
BENCHMARK(BM_SelectAll_SQLite);

// ============================================================================
// Benchmarks - Select Where
// ============================================================================

static void BM_SelectWhere_SVDB(benchmark::State& state) {
    for (auto _ : state) {
        svdb_db_t* db = nullptr;
        svdb_open(":memory:", &db);

        CreateSVDBTable(db, "t");
        InsertDataSVDB(db, "t", kNumRows);

        svdb_rows_t* rows = nullptr;
        svdb_query(db, "SELECT * FROM t WHERE x > 5000", &rows);

        int count = 0;
        while (svdb_rows_next(rows)) {
            count++;
        }
        svdb_rows_close(rows);

        svdb_close(db);
    }
}
BENCHMARK(BM_SelectWhere_SVDB);

static void BM_SelectWhere_SQLite(benchmark::State& state) {
    for (auto _ : state) {
        sqlite3* db = nullptr;
        sqlite3_open(":memory:", &db);

        CreateSQLiteTable(db, "t");
        InsertDataSQLite(db, "t", kNumRows);

        sqlite3_stmt* stmt = nullptr;
        sqlite3_prepare_v2(db, "SELECT * FROM t WHERE x > 5000", -1, &stmt, nullptr);

        int count = 0;
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            count++;
        }
        sqlite3_finalize(stmt);

        sqlite3_close(db);
    }
}
BENCHMARK(BM_SelectWhere_SQLite);

// ============================================================================
// Benchmarks - Order By
// ============================================================================

static void BM_SelectOrderBy_SVDB(benchmark::State& state) {
    for (auto _ : state) {
        svdb_db_t* db = nullptr;
        svdb_open(":memory:", &db);

        CreateSVDBTable(db, "t");
        InsertDataSVDB(db, "t", kNumRows);

        svdb_rows_t* rows = nullptr;
        svdb_query(db, "SELECT * FROM t ORDER BY x DESC", &rows);

        int count = 0;
        while (svdb_rows_next(rows)) {
            svdb_val_t v = svdb_rows_get(rows, 1);
            (void)v;
            count++;
        }
        svdb_rows_close(rows);

        svdb_close(db);
    }
}
BENCHMARK(BM_SelectOrderBy_SVDB);

static void BM_SelectOrderBy_SQLite(benchmark::State& state) {
    for (auto _ : state) {
        sqlite3* db = nullptr;
        sqlite3_open(":memory:", &db);

        CreateSQLiteTable(db, "t");
        InsertDataSQLite(db, "t", kNumRows);

        sqlite3_stmt* stmt = nullptr;
        sqlite3_prepare_v2(db, "SELECT * FROM t ORDER BY x DESC", -1, &stmt, nullptr);

        int count = 0;
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            sqlite3_column_int64(stmt, 1);
            count++;
        }
        sqlite3_finalize(stmt);

        sqlite3_close(db);
    }
}
BENCHMARK(BM_SelectOrderBy_SQLite);

// ============================================================================
// Benchmarks - Aggregate
// ============================================================================

static void BM_SelectAggregate_SVDB(benchmark::State& state) {
    for (auto _ : state) {
        svdb_db_t* db = nullptr;
        svdb_open(":memory:", &db);

        CreateSVDBTable(db, "t");
        InsertDataSVDB(db, "t", kNumRows);

        svdb_rows_t* rows = nullptr;
        svdb_query(db, "SELECT COUNT(*), SUM(x), AVG(x), MIN(x), MAX(x) FROM t", &rows);

        if (svdb_rows_next(rows)) {
            svdb_val_t v0 = svdb_rows_get(rows, 0);
            svdb_val_t v1 = svdb_rows_get(rows, 1);
            svdb_val_t v2 = svdb_rows_get(rows, 2);
            svdb_val_t v3 = svdb_rows_get(rows, 3);
            svdb_val_t v4 = svdb_rows_get(rows, 4);
            (void)v0; (void)v1; (void)v2; (void)v3; (void)v4;
        }
        svdb_rows_close(rows);

        svdb_close(db);
    }
}
BENCHMARK(BM_SelectAggregate_SVDB);

static void BM_SelectAggregate_SQLite(benchmark::State& state) {
    for (auto _ : state) {
        sqlite3* db = nullptr;
        sqlite3_open(":memory:", &db);

        CreateSQLiteTable(db, "t");
        InsertDataSQLite(db, "t", kNumRows);

        sqlite3_stmt* stmt = nullptr;
        sqlite3_prepare_v2(db, "SELECT COUNT(*), SUM(x), AVG(x), MIN(x), MAX(x) FROM t",
                           -1, &stmt, nullptr);

        if (sqlite3_step(stmt) == SQLITE_ROW) {
            sqlite3_column_int64(stmt, 0);
            sqlite3_column_int64(stmt, 1);
            sqlite3_column_double(stmt, 2);
            sqlite3_column_int64(stmt, 3);
            sqlite3_column_int64(stmt, 4);
        }
        sqlite3_finalize(stmt);

        sqlite3_close(db);
    }
}
BENCHMARK(BM_SelectAggregate_SQLite);

// ============================================================================
// Benchmarks - Join
// ============================================================================

static void CreateJoinTablesSVDB(svdb_db_t* db) {
    svdb_exec(db, "DROP TABLE IF EXISTS t1;", nullptr);
    svdb_exec(db, "DROP TABLE IF EXISTS t2;", nullptr);
    svdb_exec(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, x INTEGER);", nullptr);
    svdb_exec(db, "CREATE TABLE t2(id INTEGER PRIMARY KEY, y INTEGER);", nullptr);

    svdb_tx_t* tx = nullptr;
    svdb_begin(db, &tx);

    svdb_stmt_t* stmt1 = nullptr;
    svdb_stmt_t* stmt2 = nullptr;
    svdb_prepare(db, "INSERT INTO t1 VALUES (?, ?);", &stmt1);
    svdb_prepare(db, "INSERT INTO t2 VALUES (?, ?);", &stmt2);

    for (int i = 1; i <= kNumRows; ++i) {
        svdb_stmt_bind_int(stmt1, 1, i);
        svdb_stmt_bind_int(stmt1, 2, i % 100);
        svdb_stmt_exec(stmt1, nullptr);
        svdb_stmt_reset(stmt1);

        svdb_stmt_bind_int(stmt2, 1, i);
        svdb_stmt_bind_int(stmt2, 2, i % 100);
        svdb_stmt_exec(stmt2, nullptr);
        svdb_stmt_reset(stmt2);
    }

    svdb_stmt_close(stmt1);
    svdb_stmt_close(stmt2);
    svdb_commit(tx);
}

static void CreateJoinTablesSQLite(sqlite3* db) {
    sqlite3_exec(db, "DROP TABLE IF EXISTS t1;", nullptr, nullptr, nullptr);
    sqlite3_exec(db, "DROP TABLE IF EXISTS t2;", nullptr, nullptr, nullptr);
    sqlite3_exec(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, x INTEGER);",
                 nullptr, nullptr, nullptr);
    sqlite3_exec(db, "CREATE TABLE t2(id INTEGER PRIMARY KEY, y INTEGER);",
                 nullptr, nullptr, nullptr);

    sqlite3_exec(db, "BEGIN TRANSACTION", nullptr, nullptr, nullptr);

    sqlite3_stmt* stmt1 = nullptr;
    sqlite3_stmt* stmt2 = nullptr;
    sqlite3_prepare_v2(db, "INSERT INTO t1 VALUES (?, ?);", -1, &stmt1, nullptr);
    sqlite3_prepare_v2(db, "INSERT INTO t2 VALUES (?, ?);", -1, &stmt2, nullptr);

    for (int i = 1; i <= kNumRows; ++i) {
        sqlite3_bind_int64(stmt1, 1, i);
        sqlite3_bind_int64(stmt1, 2, i % 100);
        sqlite3_step(stmt1);
        sqlite3_reset(stmt1);

        sqlite3_bind_int64(stmt2, 1, i);
        sqlite3_bind_int64(stmt2, 2, i % 100);
        sqlite3_step(stmt2);
        sqlite3_reset(stmt2);
    }

    sqlite3_finalize(stmt1);
    sqlite3_finalize(stmt2);
    sqlite3_exec(db, "COMMIT", nullptr, nullptr, nullptr);
}

static void BM_SelectJoin_SVDB(benchmark::State& state) {
    for (auto _ : state) {
        svdb_db_t* db = nullptr;
        svdb_open(":memory:", &db);

        CreateJoinTablesSVDB(db);

        svdb_rows_t* rows = nullptr;
        svdb_query(db, "SELECT t1.id, t1.x, t2.y FROM t1 JOIN t2 ON t1.x = t2.y", &rows);

        int count = 0;
        while (svdb_rows_next(rows)) {
            svdb_val_t v0 = svdb_rows_get(rows, 0);
            svdb_val_t v1 = svdb_rows_get(rows, 1);
            svdb_val_t v2 = svdb_rows_get(rows, 2);
            (void)v0; (void)v1; (void)v2;
            count++;
        }
        svdb_rows_close(rows);

        svdb_close(db);
    }
}
BENCHMARK(BM_SelectJoin_SVDB);

static void BM_SelectJoin_SQLite(benchmark::State& state) {
    for (auto _ : state) {
        sqlite3* db = nullptr;
        sqlite3_open(":memory:", &db);

        CreateJoinTablesSQLite(db);

        sqlite3_stmt* stmt = nullptr;
        sqlite3_prepare_v2(db,
            "SELECT t1.id, t1.x, t2.y FROM t1 JOIN t2 ON t1.x = t2.y",
            -1, &stmt, nullptr);

        int count = 0;
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            sqlite3_column_int64(stmt, 0);
            sqlite3_column_int64(stmt, 1);
            sqlite3_column_int64(stmt, 2);
            count++;
        }
        sqlite3_finalize(stmt);

        sqlite3_close(db);
    }
}
BENCHMARK(BM_SelectJoin_SQLite);

// ============================================================================
// Benchmarks - Subquery
// ============================================================================

static void BM_SelectSubquery_SVDB(benchmark::State& state) {
    for (auto _ : state) {
        svdb_db_t* db = nullptr;
        svdb_open(":memory:", &db);

        CreateSVDBTable(db, "t");
        InsertDataSVDB(db, "t", kNumRows);

        svdb_rows_t* rows = nullptr;
        svdb_query(db, "SELECT * FROM t WHERE x IN (SELECT x FROM t WHERE x > 500)", &rows);

        int count = 0;
        while (svdb_rows_next(rows)) {
            count++;
        }
        svdb_rows_close(rows);

        svdb_close(db);
    }
}
BENCHMARK(BM_SelectSubquery_SVDB);

static void BM_SelectSubquery_SQLite(benchmark::State& state) {
    for (auto _ : state) {
        sqlite3* db = nullptr;
        sqlite3_open(":memory:", &db);

        CreateSQLiteTable(db, "t");
        InsertDataSQLite(db, "t", kNumRows);

        sqlite3_stmt* stmt = nullptr;
        sqlite3_prepare_v2(db,
            "SELECT * FROM t WHERE x IN (SELECT x FROM t WHERE x > 500)",
            -1, &stmt, nullptr);

        int count = 0;
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            count++;
        }
        sqlite3_finalize(stmt);

        sqlite3_close(db);
    }
}
BENCHMARK(BM_SelectSubquery_SQLite);

// ============================================================================
// Benchmarks - Update
// ============================================================================

static void BM_Update_SVDB(benchmark::State& state) {
    for (auto _ : state) {
        svdb_db_t* db = nullptr;
        svdb_open(":memory:", &db);

        CreateSVDBTable(db, "t");
        InsertDataSVDB(db, "t", kNumRows);

        svdb_exec(db, "UPDATE t SET x = x + 1 WHERE id <= 500", nullptr);

        svdb_rows_t* rows = nullptr;
        svdb_query(db, "SELECT x FROM t WHERE id = 250", &rows);
        if (svdb_rows_next(rows)) {
            svdb_val_t v = svdb_rows_get(rows, 0);
            (void)v;
        }
        svdb_rows_close(rows);

        svdb_close(db);
    }
}
BENCHMARK(BM_Update_SVDB);

static void BM_Update_SQLite(benchmark::State& state) {
    for (auto _ : state) {
        sqlite3* db = nullptr;
        sqlite3_open(":memory:", &db);

        CreateSQLiteTable(db, "t");
        InsertDataSQLite(db, "t", kNumRows);

        sqlite3_exec(db, "UPDATE t SET x = x + 1 WHERE id <= 500", nullptr, nullptr, nullptr);

        sqlite3_stmt* stmt = nullptr;
        sqlite3_prepare_v2(db, "SELECT x FROM t WHERE id = 250", -1, &stmt, nullptr);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            sqlite3_column_int64(stmt, 0);
        }
        sqlite3_finalize(stmt);

        sqlite3_close(db);
    }
}
BENCHMARK(BM_Update_SQLite);

// ============================================================================
// Benchmarks - Delete
// ============================================================================

static void BM_Delete_SVDB(benchmark::State& state) {
    for (auto _ : state) {
        svdb_db_t* db = nullptr;
        svdb_open(":memory:", &db);

        CreateSVDBTable(db, "t");
        InsertDataSVDB(db, "t", kNumRows);

        svdb_exec(db, "DELETE FROM t WHERE id > 500", nullptr);

        svdb_rows_t* rows = nullptr;
        svdb_query(db, "SELECT COUNT(*) FROM t", &rows);
        if (svdb_rows_next(rows)) {
            svdb_val_t v = svdb_rows_get(rows, 0);
            (void)v;
        }
        svdb_rows_close(rows);

        svdb_close(db);
    }
}
BENCHMARK(BM_Delete_SVDB);

static void BM_Delete_SQLite(benchmark::State& state) {
    for (auto _ : state) {
        sqlite3* db = nullptr;
        sqlite3_open(":memory:", &db);

        CreateSQLiteTable(db, "t");
        InsertDataSQLite(db, "t", kNumRows);

        sqlite3_exec(db, "DELETE FROM t WHERE id > 500", nullptr, nullptr, nullptr);

        sqlite3_stmt* stmt = nullptr;
        sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM t", -1, &stmt, nullptr);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            sqlite3_column_int64(stmt, 0);
        }
        sqlite3_finalize(stmt);

        sqlite3_close(db);
    }
}
BENCHMARK(BM_Delete_SQLite);

// ============================================================================
// Main
// ============================================================================

BENCHMARK_MAIN();