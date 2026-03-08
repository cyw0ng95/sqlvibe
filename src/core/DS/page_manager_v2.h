// PageManagerV2 - C++ owned page manager with direct file I/O
// v0.11.3: No Go callbacks - C++ owns all I/O operations
#ifndef SVDB_DS_PAGE_MANAGER_V2_H
#define SVDB_DS_PAGE_MANAGER_V2_H

#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <fstream>
#include <memory>
#include <atomic>
#include "page.h"
#include "cache_v2.h"
#include "freelist_v2.h"

namespace svdb::ds {

// Database header structure (SQLite-compatible)
struct DBHeader {
    static constexpr uint32_t MAGIC = 0x53514C69;  // "SQLi"
    static constexpr uint32_t VERSION = 1;
    
    uint32_t magic;
    uint32_t version;
    uint32_t page_size;
    uint32_t page_count;
    uint32_t schema_version;
    uint32_t freelist_head;
    uint32_t freelist_count;
    uint32_t reserved_space;
    uint8_t  write_version;
    uint8_t  read_version;
    uint8_t  reserved[2];
    uint64_t schema_cookie;
    uint64_t file_format;
    uint64_t reserved2[3];
};

static_assert(sizeof(DBHeader) <= 100, "DBHeader must fit in first page");

class PageManagerV2 {
public:
    // Create new database or open existing
    explicit PageManagerV2(const std::string& db_path, 
                           uint32_t page_size = 4096,
                           bool create_if_missing = true);
    ~PageManagerV2();
    
    // Non-copyable
    PageManagerV2(const PageManagerV2&) = delete;
    PageManagerV2& operator=(const PageManagerV2&) = delete;
    
    // Page I/O operations (direct, no callbacks)
    // Returns allocated buffer, caller must free with FreePageBuffer
    uint8_t* ReadPage(uint32_t page_num, size_t* out_size);
    void WritePage(uint32_t page_num, const uint8_t* data, size_t size);
    
    // Free a page buffer returned by ReadPage
    static void FreePageBuffer(uint8_t* page);
    
    // Page allocation
    uint32_t AllocatePage();
    void FreePage(uint32_t page_num);
    
    // Database info
    uint32_t GetPageSize() const { return page_size_; }
    uint32_t GetPageCount() const { return page_count_.load(); }
    uint64_t GetFileSize() const;
    
    // Sync and checkpoint
    void Sync();
    void Checkpoint();
    
    // Cache management
    void ClearCache();
    size_t GetCacheSize() const;

    // Database lifecycle
    void Close();
    bool IsOpen() const { return is_open_.load(); }

    // Header access
    DBHeader GetHeader() const;
    void UpdateHeader(const DBHeader& header);

    // =========================================================================
    // WS3: Memory-Mapped I/O Support
    // =========================================================================

    // Enable mmap for read-only access (faster sequential scans)
    void EnableMMap(bool enable = true, bool use_huge_pages = false);

    // Check if mmap is enabled
    bool IsMMapEnabled() const { return mmap_enabled_; }

    // Get direct pointer to mmap'd page (only valid if mmap enabled)
    // Returns nullptr if page is not mmap'd or mmap is disabled
    const uint8_t* GetMMapPage(uint32_t page_num);

    // Prefetch pages for sequential scan
    void PrefetchPages(uint32_t start_page, uint32_t count);

    // Advise access pattern for kernel
    void AdviseSequential();
    void AdviseRandom();
    void AdviseWillNeed(uint32_t start_page, uint32_t count);
    void AdviseDontNeed(uint32_t start_page, uint32_t count);

private:
    // Internal methods
    void InitializeDatabase();
    void LoadHeader();
    void SaveHeader();
    void EnsureCapacity(uint32_t min_pages);
    
    // File operations
    bool OpenFile();
    void CloseFile();
    size_t ReadFromFile(uint64_t offset, void* buffer, size_t size);
    void WriteToFile(uint64_t offset, const void* buffer, size_t size);
    
    // Members
    std::string db_path_;
    std::fstream file_;
    uint32_t page_size_;
    std::atomic<uint32_t> page_count_;
    std::atomic<bool> is_open_;

    // C++ owned components (no callbacks)
    std::unique_ptr<LRUCacheV2> cache_;
    std::unique_ptr<FreeListV2> freelist_;
    std::mutex mutex_;

    // Header cache
    DBHeader header_;

    // =========================================================================
    // WS3: Memory-Mapped I/O members
    // =========================================================================
    bool mmap_enabled_;
    bool mmap_huge_pages_;
    void* mmap_data_;
    size_t mmap_size_;
    int mmap_fd_;  // Separate fd for mmap
};

// C API for Go bindings (type-safe, no callbacks)
extern "C" {
    struct svdb_page_manager_v2_t;
    
    svdb_page_manager_v2_t* svdb_pm_v2_create(const char* path, uint32_t page_size);
    void svdb_pm_v2_destroy(svdb_page_manager_v2_t* pm);
    
    // Page I/O
    uint8_t* svdb_pm_v2_read_page(svdb_page_manager_v2_t* pm, uint32_t page_num, size_t* out_size);
    void svdb_pm_v2_write_page(svdb_page_manager_v2_t* pm, uint32_t page_num, const uint8_t* data, size_t size);
    
    // Allocation
    uint32_t svdb_pm_v2_allocate_page(svdb_page_manager_v2_t* pm);
    void svdb_pm_v2_free_page(svdb_page_manager_v2_t* pm, uint32_t page_num);
    
    // Info
    uint32_t svdb_pm_v2_get_page_size(svdb_page_manager_v2_t* pm);
    uint32_t svdb_pm_v2_get_page_count(svdb_page_manager_v2_t* pm);
    uint64_t svdb_pm_v2_get_file_size(svdb_page_manager_v2_t* pm);
    
    // Lifecycle
    void svdb_pm_v2_sync(svdb_page_manager_v2_t* pm);
    void svdb_pm_v2_close(svdb_page_manager_v2_t* pm);
    int svdb_pm_v2_is_open(svdb_page_manager_v2_t* pm);
    
    // Cache
    void svdb_pm_v2_clear_cache(svdb_page_manager_v2_t* pm);
    size_t svdb_pm_v2_get_cache_size(svdb_page_manager_v2_t* pm);
}

}  // namespace svdb::ds

#endif  // SVDB_DS_PAGE_MANAGER_V2_H
