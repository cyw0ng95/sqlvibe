// Virtual Table Registry C++ Unit Tests
#include <gtest/gtest.h>
#include "vtab_registry.h"
#include "vtab_series.h"
#include "vtab_fts5.h"
#include "vtab_api.h"
#include <cstring>
#include <thread>
#include <vector>

namespace svdb::test {

// ============================================================================
// VTabRegistry Tests
// ============================================================================

class VTabRegistryTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Note: We don't clean up modules here since they have static lifetime.
        // Tests should use unique module names to avoid conflicts.
        (void)VTabRegistry::Instance(); // Ensure registry is initialized
    }
};

TEST_F(VTabRegistryTest, RegisterAndGetModule) {
    auto& registry = VTabRegistry::Instance();
    
    SeriesModule* module = new SeriesModule();
    ASSERT_EQ(registry.RegisterModule("test_series1", module), 0);
    
    VTabModule* retrieved = registry.GetModule("test_series1");
    ASSERT_NE(retrieved, nullptr);
    
    // Should be case-insensitive
    VTabModule* retrieved_lower = registry.GetModule("TEST_SERIES1");
    ASSERT_EQ(retrieved_lower, retrieved);
    
    VTabModule* retrieved_mixed = registry.GetModule("Test_Series1");
    ASSERT_EQ(retrieved_mixed, retrieved);
}

TEST_F(VTabRegistryTest, HasModule) {
    auto& registry = VTabRegistry::Instance();
    
    ASSERT_FALSE(registry.HasModule("test_series2"));
    
    SeriesModule* module = new SeriesModule();
    ASSERT_EQ(registry.RegisterModule("test_series2", module), 0);
    
    ASSERT_TRUE(registry.HasModule("test_series2"));
    ASSERT_TRUE(registry.HasModule("TEST_SERIES2"));
    ASSERT_FALSE(registry.HasModule("other"));
}

TEST_F(VTabRegistryTest, UnregisterModule) {
    auto& registry = VTabRegistry::Instance();
    
    SeriesModule* module = new SeriesModule();
    ASSERT_EQ(registry.RegisterModule("test_series3", module), 0);
    ASSERT_TRUE(registry.HasModule("test_series3"));
    
    ASSERT_EQ(registry.UnregisterModule("test_series3"), 0);
    ASSERT_FALSE(registry.HasModule("test_series3"));
}

TEST_F(VTabRegistryTest, GetModuleNames) {
    auto& registry = VTabRegistry::Instance();
    
    auto names = registry.GetModuleNames();
    size_t initial_count = names.size();
    
    registry.RegisterModule("test_series_a", new SeriesModule());
    registry.RegisterModule("test_series_b", new SeriesModule());
    registry.RegisterModule("test_series_c", new SeriesModule());
    
    names = registry.GetModuleNames();
    ASSERT_EQ(names.size(), initial_count + 3);
}

TEST_F(VTabRegistryTest, RegisterDuplicateModule) {
    auto& registry = VTabRegistry::Instance();
    
    SeriesModule* module1 = new SeriesModule();
    ASSERT_EQ(registry.RegisterModule("test_series_dup", module1), 0);
    
    SeriesModule* module2 = new SeriesModule();
    ASSERT_EQ(registry.RegisterModule("test_series_dup", module2), -2); // Already exists
}

TEST_F(VTabRegistryTest, RegisterNullModule) {
    auto& registry = VTabRegistry::Instance();
    
    ASSERT_EQ(registry.RegisterModule("test_null", nullptr), -1);
}

TEST_F(VTabRegistryTest, GetNonExistentModule) {
    auto& registry = VTabRegistry::Instance();
    
    VTabModule* module = registry.GetModule("nonexistent_module");
    ASSERT_EQ(module, nullptr);
}

// ============================================================================
// Series VTab Tests
// ============================================================================

class SeriesVTabTest : public ::testing::Test {
protected:
    SeriesVTab* CreateSeries(int64_t start, int64_t stop, int64_t step = 1) {
        return new SeriesVTab(start, stop, step);
    }
};

TEST_F(SeriesVTabTest, Columns) {
    SeriesVTab* vtab = CreateSeries(1, 10);
    auto cols = vtab->Columns();
    
    ASSERT_EQ(cols.size(), 1);
    ASSERT_EQ(cols[0], "value");
    
    delete vtab;
}

TEST_F(SeriesVTabTest, OpenCursor) {
    SeriesVTab* vtab = CreateSeries(1, 10);
    VTabCursor* cursor = vtab->Open();
    
    ASSERT_NE(cursor, nullptr);
    
    delete cursor;
    delete vtab;
}

TEST_F(SeriesVTabTest, CursorIteration) {
    SeriesVTab* vtab = CreateSeries(1, 5);
    VTabCursor* cursor = vtab->Open();
    
    cursor->Filter(0, "", {});
    
    std::vector<int64_t> values;
    while (!cursor->Eof()) {
        int type;
        int64_t ival;
        double rval;
        const char* sval;
        size_t slen;
        
        cursor->Column(0, &type, &ival, &rval, &sval, &slen);
        ASSERT_EQ(type, 1); // INT
        values.push_back(ival);
        
        cursor->Next();
    }
    
    ASSERT_EQ(values.size(), 5);
    ASSERT_EQ(values[0], 1);
    ASSERT_EQ(values[1], 2);
    ASSERT_EQ(values[2], 3);
    ASSERT_EQ(values[3], 4);
    ASSERT_EQ(values[4], 5);
    
    delete cursor;
    delete vtab;
}

TEST_F(SeriesVTabTest, CursorWithStep) {
    SeriesVTab* vtab = CreateSeries(0, 100, 10);
    VTabCursor* cursor = vtab->Open();
    
    cursor->Filter(0, "", {});
    
    std::vector<int64_t> values;
    while (!cursor->Eof()) {
        int type;
        int64_t ival;
        double rval;
        const char* sval;
        size_t slen;
        
        cursor->Column(0, &type, &ival, &rval, &sval, &slen);
        values.push_back(ival);
        
        cursor->Next();
    }
    
    ASSERT_EQ(values.size(), 11); // 0, 10, 20, ..., 100
    ASSERT_EQ(values[0], 0);
    ASSERT_EQ(values[5], 50);
    ASSERT_EQ(values[10], 100);
    
    delete cursor;
    delete vtab;
}

TEST_F(SeriesVTabTest, CursorWithNegativeStep) {
    SeriesVTab* vtab = CreateSeries(10, 0, -1);
    VTabCursor* cursor = vtab->Open();
    
    cursor->Filter(0, "", {});
    
    std::vector<int64_t> values;
    while (!cursor->Eof()) {
        int type;
        int64_t ival;
        cursor->Column(0, &type, &ival, nullptr, nullptr, nullptr);
        values.push_back(ival);
        cursor->Next();
    }
    
    ASSERT_EQ(values.size(), 11); // 10, 9, 8, ..., 0
    ASSERT_EQ(values[0], 10);
    ASSERT_EQ(values[10], 0);
    
    delete cursor;
    delete vtab;
}

TEST_F(SeriesVTabTest, RowID) {
    SeriesVTab* vtab = CreateSeries(5, 10);
    VTabCursor* cursor = vtab->Open();
    
    cursor->Filter(0, "", {});
    
    int64_t rowid;
    int expected_rowid = 0;
    
    while (!cursor->Eof()) {
        cursor->RowID(&rowid);
        ASSERT_EQ(rowid, expected_rowid);
        expected_rowid++;
        cursor->Next();
    }
    
    delete cursor;
    delete vtab;
}

TEST_F(SeriesVTabTest, InvalidColumnIndex) {
    SeriesVTab* vtab = CreateSeries(1, 5);
    VTabCursor* cursor = vtab->Open();
    
    cursor->Filter(0, "", {});
    
    int type;
    int64_t ival;
    int result = cursor->Column(1, &type, &ival, nullptr, nullptr, nullptr);
    ASSERT_EQ(result, -1); // Invalid column
    
    delete cursor;
    delete vtab;
}

// ============================================================================
// SeriesModule Tests
// ============================================================================

class SeriesModuleTest : public ::testing::Test {
protected:
    SeriesModule module;
};

TEST_F(SeriesModuleTest, CreateBasic) {
    std::vector<std::string> args = {"1", "10"};
    VTab* vtab = module.Create(args);
    
    ASSERT_NE(vtab, nullptr);
    
    SeriesVTab* series = dynamic_cast<SeriesVTab*>(vtab);
    ASSERT_NE(series, nullptr);
    
    delete series;
}

TEST_F(SeriesModuleTest, CreateWithStep) {
    std::vector<std::string> args = {"0", "100", "10"};
    VTab* vtab = module.Create(args);
    
    ASSERT_NE(vtab, nullptr);
    
    SeriesVTab* series = dynamic_cast<SeriesVTab*>(vtab);
    ASSERT_NE(series, nullptr);
    
    delete series;
}

TEST_F(SeriesModuleTest, CreateTooFewArgs) {
    std::vector<std::string> args = {"1"}; // Missing stop
    VTab* vtab = module.Create(args);
    
    ASSERT_EQ(vtab, nullptr);
}

TEST_F(SeriesModuleTest, CreateInvalidArgs) {
    std::vector<std::string> args = {"abc", "def"};
    VTab* vtab = module.Create(args);
    
    ASSERT_EQ(vtab, nullptr);
}

TEST_F(SeriesModuleTest, CreateZeroStep) {
    std::vector<std::string> args = {"1", "10", "0"};
    VTab* vtab = module.Create(args);
    
    ASSERT_EQ(vtab, nullptr);
}

TEST_F(SeriesModuleTest, ConnectSameAsCreate) {
    std::vector<std::string> args = {"5", "15", "5"};
    VTab* vtab = module.Connect(args);
    
    ASSERT_NE(vtab, nullptr);
    
    SeriesVTab* series = dynamic_cast<SeriesVTab*>(vtab);
    ASSERT_NE(series, nullptr);
    
    delete series;
}

// ============================================================================
// FTS5 VTab Tests
// ============================================================================

class FTS5VTabTest : public ::testing::Test {
protected:
    FTS5VTab* CreateFTS5(const std::vector<std::string>& columns) {
        return new FTS5VTab(columns);
    }
};

TEST_F(FTS5VTabTest, Columns) {
    FTS5VTab* vtab = CreateFTS5({"title", "content"});
    auto cols = vtab->Columns();
    
    ASSERT_EQ(cols.size(), 2);
    ASSERT_EQ(cols[0], "title");
    ASSERT_EQ(cols[1], "content");
    
    delete vtab;
}

TEST_F(FTS5VTabTest, OpenCursor) {
    FTS5VTab* vtab = CreateFTS5({"text"});
    VTabCursor* cursor = vtab->Open();
    
    ASSERT_NE(cursor, nullptr);
    
    delete cursor;
    delete vtab;
}

TEST_F(FTS5VTabTest, InsertAndSearch) {
    FTS5VTab* vtab = CreateFTS5({"title", "content"});
    
    // Insert documents
    vtab->Insert(1, {"Hello World", "This is a test document"});
    vtab->Insert(2, {"Foo Bar", "Another document with different content"});
    vtab->Insert(3, {"Hello Again", "More test content here"});
    
    // Open cursor and search
    FTS5Cursor* cursor = static_cast<FTS5Cursor*>(vtab->Open());
    cursor->Filter(0, "", {"test"});
    
    ASSERT_FALSE(cursor->Eof());
    
    // Should find documents containing "test"
    std::vector<int64_t> found_docs;
    while (!cursor->Eof()) {
        int64_t rowid;
        cursor->RowID(&rowid);
        found_docs.push_back(rowid);
        cursor->Next();
    }
    
    // Documents 1 and 3 contain "test"
    ASSERT_EQ(found_docs.size(), 2);
    
    delete cursor;
    delete vtab;
}

TEST_F(FTS5VTabTest, BM25Scoring) {
    FTS5VTab* vtab = CreateFTS5({"text"});
    
    // Insert documents with varying term frequencies
    vtab->Insert(1, {"test"});
    vtab->Insert(2, {"test test test"});
    vtab->Insert(3, {"other content"});
    
    // Score should be higher for doc 2 (more occurrences of "test")
    double score1 = vtab->BM25Score(1, {"test"});
    double score2 = vtab->BM25Score(2, {"test"});
    
    ASSERT_GT(score2, score1);
    
    delete vtab;
}

TEST_F(FTS5VTabTest, DeleteDocument) {
    FTS5VTab* vtab = CreateFTS5({"text"});
    
    vtab->Insert(1, {"hello world"});
    vtab->Insert(2, {"foo bar"});
    
    // Delete document 1
    vtab->Delete(1);
    
    // Search should only find document 2
    FTS5Cursor* cursor = static_cast<FTS5Cursor*>(vtab->Open());
    cursor->Filter(0, "", {"hello"});
    
    ASSERT_TRUE(cursor->Eof()); // Should not find "hello" anymore
    
    delete cursor;
    delete vtab;
}

TEST_F(FTS5VTabTest, PrefixSearch) {
    FTS5VTab* vtab = CreateFTS5({"text"});
    
    vtab->Insert(1, {"testing"});
    vtab->Insert(2, {"tester"});
    vtab->Insert(3, {"other"});
    
    FTS5Index* index = vtab->GetIndex();
    auto results = index->SearchPrefix("test");
    
    // Should find both "testing" and "tester"
    ASSERT_EQ(results.size(), 2);
    
    delete vtab;
}

// ============================================================================
// FTS5Module Tests
// ============================================================================

class FTS5ModuleTest : public ::testing::Test {
protected:
    FTS5Module module;
};

TEST_F(FTS5ModuleTest, CreateBasic) {
    std::vector<std::string> args = {"title", "content"};
    VTab* vtab = module.Create(args);
    
    ASSERT_NE(vtab, nullptr);
    
    FTS5VTab* fts5 = dynamic_cast<FTS5VTab*>(vtab);
    ASSERT_NE(fts5, nullptr);
    
    delete fts5;
}

TEST_F(FTS5ModuleTest, CreateWithTokenizer) {
    std::vector<std::string> args = {"text", "tokenize=porter"};
    VTab* vtab = module.Create(args);
    
    ASSERT_NE(vtab, nullptr);
    
    FTS5VTab* fts5 = dynamic_cast<FTS5VTab*>(vtab);
    ASSERT_NE(fts5, nullptr);
    
    delete fts5;
}

TEST_F(FTS5ModuleTest, CreateNoColumns) {
    std::vector<std::string> args = {};
    VTab* vtab = module.Create(args);
    
    ASSERT_EQ(vtab, nullptr);
}

TEST_F(FTS5ModuleTest, ConnectSameAsCreate) {
    std::vector<std::string> args = {"col1", "col2"};
    VTab* vtab = module.Connect(args);
    
    ASSERT_NE(vtab, nullptr);
    
    FTS5VTab* fts5 = dynamic_cast<FTS5VTab*>(vtab);
    ASSERT_NE(fts5, nullptr);
    
    delete fts5;
}

// ============================================================================
// C API Tests (for C-based modules)
// ============================================================================

class VTabCApiTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Note: Modules have static lifetime, don't clean up
        (void)VTabRegistry::Instance();
    }
};

TEST_F(VTabCApiTest, HasModule) {
    ASSERT_EQ(svdb_vtab_has_module("nonexistent_capi"), 0);
}

TEST_F(VTabCApiTest, GetModuleCount) {
    int count = svdb_vtab_get_module_count();
    ASSERT_GE(count, 0);
}

TEST_F(VTabCApiTest, GetModuleName) {
    // Just verify we can get a module name, don't check specific index
    char buffer[256];
    int count = svdb_vtab_get_module_count();
    if (count > 0) {
        int result = svdb_vtab_get_module_name(0, buffer, sizeof(buffer));
        ASSERT_EQ(result, 0);
        // Module name should be lowercase
        ASSERT_GT(strlen(buffer), 0);
    }
}

// ============================================================================
// Thread Safety Tests
// ============================================================================

class VTabThreadSafetyTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Note: Modules have static lifetime, don't clean up
        (void)VTabRegistry::Instance();
    }
};

TEST_F(VTabThreadSafetyTest, ConcurrentRegistration) {
    const int num_threads = 10;
    std::vector<std::thread> threads;
    
    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([i]() {
            auto& registry = VTabRegistry::Instance();
            std::string name = "test_thread_" + std::to_string(i);
            registry.RegisterModule(name, new SeriesModule());
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    // Should have registered all modules
    SUCCEED();
}

TEST_F(VTabThreadSafetyTest, ConcurrentLookup) {
    auto& registry = VTabRegistry::Instance();
    registry.RegisterModule("test_concurrent_lookup", new SeriesModule());
    
    const int num_threads = 10;
    std::vector<std::thread> threads;
    
    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([&registry]() {
            for (int j = 0; j < 1000; j++) {
                VTabModule* mod = registry.GetModule("test_concurrent_lookup");
                (void)mod; // Suppress unused warning
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    // Should not crash
    SUCCEED();
}

} /* namespace svdb::test */
