// PageManagerV2 C++ Unit Tests
#include <gtest/gtest.h>
#include "page_manager_v2.h"
#include "arena_v2.h"
#include "cache_v2.h"
#include "freelist_v2.h"
#include <cstring>
#include <filesystem>
#include <vector>

namespace svdb::ds::test {

namespace fs = std::filesystem;

// ============================================================================
// ArenaV2 Tests
// ============================================================================

class ArenaV2Test : public ::testing::Test {
protected:
    void SetUp() override {
        arena_ = std::make_unique<ArenaV2>(4096);
    }
    
    void TearDown() override {
        arena_.reset();
    }
    
    std::unique_ptr<ArenaV2> arena_;
};

TEST_F(ArenaV2Test, AllocBasic) {
    void* ptr = arena_->Alloc(100);
    ASSERT_NE(ptr, nullptr);
    
    // Write and read back
    std::memset(ptr, 0xAB, 100);
    uint8_t* bytes = static_cast<uint8_t*>(ptr);
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(bytes[i], 0xAB);
    }
}

TEST_F(ArenaV2Test, AllocAlignment) {
    void* ptr1 = arena_->Alloc(1);
    void* ptr2 = arena_->Alloc(1);
    
    // Should be 8-byte aligned
    ASSERT_EQ(reinterpret_cast<uintptr_t>(ptr1) % 8, 0);
    ASSERT_EQ(reinterpret_cast<uintptr_t>(ptr2) % 8, 0);
}

TEST_F(ArenaV2Test, CallocZeroInit) {
    void* ptr = arena_->Calloc(10, sizeof(int));
    ASSERT_NE(ptr, nullptr);
    
    int* ints = static_cast<int*>(ptr);
    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(ints[i], 0);
    }
}

TEST_F(ArenaV2Test, Reset) {
    arena_->Alloc(1000);
    size_t used_before = arena_->BytesUsed();
    ASSERT_GT(used_before, 0);
    
    arena_->Reset();
    ASSERT_EQ(arena_->BytesUsed(), 0);
}

TEST_F(ArenaV2Test, MultipleChunks) {
    // Allocate more than one chunk
    for (int i = 0; i < 100; i++) {
        arena_->Alloc(10000);
    }
    
    ASSERT_GT(arena_->ChunkCount(), 1);
    ASSERT_GT(arena_->BytesAllocated(), 100 * 10000);
}

// ============================================================================
// FreeListV2 Tests
// ============================================================================

class FreeListV2Test : public ::testing::Test {
protected:
    void SetUp() override {
        freelist_ = std::make_unique<FreeListV2>();
    }
    
    std::unique_ptr<FreeListV2> freelist_;
};

TEST_F(FreeListV2Test, AddAndAllocate) {
    freelist_->Add(5);
    freelist_->Add(10);
    freelist_->Add(15);
    
    ASSERT_EQ(freelist_->Count(), 3);
    
    uint32_t page = freelist_->Allocate();
    ASSERT_EQ(page, 15);  // LIFO
    ASSERT_EQ(freelist_->Count(), 2);
    
    page = freelist_->Allocate();
    ASSERT_EQ(page, 10);
    ASSERT_EQ(freelist_->Count(), 1);
    
    page = freelist_->Allocate();
    ASSERT_EQ(page, 5);
    ASSERT_EQ(freelist_->Count(), 0);
}

TEST_F(FreeListV2Test, AllocateEmpty) {
    uint32_t page = freelist_->Allocate();
    ASSERT_EQ(page, 0);
}

TEST_F(FreeListV2Test, Clear) {
    freelist_->Add(5);
    freelist_->Add(10);
    freelist_->Clear();
    ASSERT_EQ(freelist_->Count(), 0);
}

// ============================================================================
// LRUCacheV2 Tests
// ============================================================================

class LRUCacheV2Test : public ::testing::Test {
protected:
    void SetUp() override {
        cache_ = std::make_unique<LRUCacheV2>(10);  // 10 pages
    }
    
    std::unique_ptr<LRUCacheV2> cache_;
};

TEST_F(LRUCacheV2Test, PutAndGet) {
    std::vector<uint8_t> data(100, 0xCD);
    cache_->Put(1, data.data(), data.size());
    
    size_t size = 0;
    uint8_t* retrieved = cache_->Get(1, &size);
    ASSERT_NE(retrieved, nullptr);
    ASSERT_EQ(size, 100);
    ASSERT_EQ(std::memcmp(retrieved, data.data(), 100), 0);
    LRUCacheV2::FreePage(retrieved);
}

TEST_F(LRUCacheV2Test, GetMissing) {
    size_t size = 0;
    uint8_t* retrieved = cache_->Get(999, &size);
    ASSERT_EQ(retrieved, nullptr);
}

TEST_F(LRUCacheV2Test, Remove) {
    std::vector<uint8_t> data(100, 0xAB);
    cache_->Put(1, data.data(), data.size());
    cache_->Remove(1);
    
    size_t size = 0;
    uint8_t* retrieved = cache_->Get(1, &size);
    ASSERT_EQ(retrieved, nullptr);
}

TEST_F(LRUCacheV2Test, Clear) {
    for (uint32_t i = 1; i <= 5; i++) {
        std::vector<uint8_t> data(100, static_cast<uint8_t>(i));
        cache_->Put(i, data.data(), data.size());
    }
    
    ASSERT_EQ(cache_->Size(), 5);
    cache_->Clear();
    ASSERT_EQ(cache_->Size(), 0);
}

TEST_F(LRUCacheV2Test, HitRate) {
    std::vector<uint8_t> data(100, 0xFF);
    cache_->Put(1, data.data(), data.size());
    
    // Miss
    size_t size = 0;
    cache_->Get(999, &size);
    // Hit
    cache_->Get(1, &size);
    // Hit
    cache_->Get(1, &size);
    
    ASSERT_EQ(cache_->Hits(), 2);
    ASSERT_EQ(cache_->Misses(), 1);
    ASSERT_NEAR(cache_->HitRate(), 2.0/3.0, 0.01);
}

// ============================================================================
// PageManagerV2 Tests
// ============================================================================

class PageManagerV2Test : public ::testing::Test {
protected:
    void SetUp() override {
        test_db_ = "/tmp/test_pm_v2_" + std::to_string(getpid()) + ".db";
        // Clean up any existing file
        std::error_code ec;
        fs::remove(test_db_, ec);
    }
    
    void TearDown() override {
        std::error_code ec;
        fs::remove(test_db_, ec);
    }
    
    std::string test_db_;
};

TEST_F(PageManagerV2Test, CreateAndOpen) {
    {
        PageManagerV2 pm(test_db_, 4096, true);
        ASSERT_TRUE(pm.IsOpen());
        ASSERT_EQ(pm.GetPageSize(), 4096);
        ASSERT_EQ(pm.GetPageCount(), 1);  // Header page
    }
    
    // Reopen
    {
        PageManagerV2 pm(test_db_, 4096, false);
        ASSERT_TRUE(pm.IsOpen());
        ASSERT_EQ(pm.GetPageSize(), 4096);
    }
}

TEST_F(PageManagerV2Test, AllocatePage) {
    PageManagerV2 pm(test_db_, 4096, true);
    
    uint32_t page1 = pm.AllocatePage();
    ASSERT_EQ(page1, 2);  // First page after header
    
    uint32_t page2 = pm.AllocatePage();
    ASSERT_EQ(page2, 3);
    
    ASSERT_EQ(pm.GetPageCount(), 3);
}

TEST_F(PageManagerV2Test, FreePage) {
    PageManagerV2 pm(test_db_, 4096, true);
    
    uint32_t page1 = pm.AllocatePage();
    uint32_t page2 = pm.AllocatePage();
    
    pm.FreePage(page1);
    
    // Next allocation should reuse freed page
    uint32_t page3 = pm.AllocatePage();
    ASSERT_EQ(page3, page1);
}

TEST_F(PageManagerV2Test, ReadWritePage) {
    PageManagerV2 pm(test_db_, 4096, true);
    
    uint32_t page_num = pm.AllocatePage();
    
    // Write data
    std::vector<uint8_t> data(4096, 0xDE);
    pm.WritePage(page_num, data.data(), data.size());
    
    // Read back
    size_t size = 0;
    uint8_t* read_data = pm.ReadPage(page_num, &size);
    ASSERT_NE(read_data, nullptr);
    ASSERT_EQ(size, 4096);
    ASSERT_EQ(std::memcmp(read_data, data.data(), 4096), 0);
    PageManagerV2::FreePageBuffer(read_data);
}

TEST_F(PageManagerV2Test, Persistence) {
    // Write data
    {
        PageManagerV2 pm(test_db_, 4096, true);
        uint32_t page_num = pm.AllocatePage();
        
        std::vector<uint8_t> data(4096, 0xAD);
        pm.WritePage(page_num, data.data(), data.size());
        pm.Sync();
    }
    
    // Read data after reopen
    {
        PageManagerV2 pm(test_db_, 4096, false);
        size_t size = 0;
        uint8_t* data = pm.ReadPage(2, &size);
        
        std::vector<uint8_t> expected(4096, 0xAD);
        ASSERT_EQ(std::memcmp(data, expected.data(), 4096), 0);
        PageManagerV2::FreePageBuffer(data);
    }
}

TEST_F(PageManagerV2Test, Cache) {
    PageManagerV2 pm(test_db_, 4096, true);
    
    uint32_t page_num = pm.AllocatePage();
    
    // Write and read (populates cache)
    std::vector<uint8_t> data(4096, 0xBE);
    pm.WritePage(page_num, data.data(), data.size());
    
    size_t size = 0;
    pm.ReadPage(page_num, &size);  // Should be in cache now
    
    ASSERT_EQ(pm.GetCacheSize(), 1);
    
    pm.ClearCache();
    ASSERT_EQ(pm.GetCacheSize(), 0);
}

TEST_F(PageManagerV2Test, GetFileSize) {
    PageManagerV2 pm(test_db_, 4096, true);
    
    // Initial size (1 page)
    ASSERT_EQ(pm.GetFileSize(), 4096);
    
    // Allocate more pages
    pm.AllocatePage();
    pm.AllocatePage();
    pm.Sync();
    
    ASSERT_EQ(pm.GetFileSize(), 4096 * 3);
}

TEST_F(PageManagerV2Test, HeaderPersistence) {
    {
        PageManagerV2 pm(test_db_, 8192, true);  // Custom page size
        pm.AllocatePage();
        pm.AllocatePage();
    }
    
    // Reopen and verify
    {
        PageManagerV2 pm(test_db_, 4096, false);  // Page size param ignored on open
        ASSERT_EQ(pm.GetPageSize(), 8192);  // Should read from file
        ASSERT_EQ(pm.GetPageCount(), 3);
    }
}

}  // namespace svdb::ds::test
