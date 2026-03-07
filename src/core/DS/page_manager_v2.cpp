// PageManagerV2 Implementation - Direct file I/O, no Go callbacks
#include "page_manager_v2.h"
#include <cstring>
#include <stdexcept>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

namespace svdb::ds {

// ============================================================================
// C API Implementation
// ============================================================================

extern "C" {

struct svdb_page_manager_v2_t {
    PageManagerV2* ptr;
};

svdb_page_manager_v2_t* svdb_pm_v2_create(const char* path, uint32_t page_size) {
    try {
        auto* pm = new PageManagerV2(path, page_size, true);
        auto* handle = new svdb_page_manager_v2_t{pm};
        return handle;
    } catch (...) {
        return nullptr;
    }
}

void svdb_pm_v2_destroy(svdb_page_manager_v2_t* pm) {
    if (pm) {
        delete pm->ptr;
        delete pm;
    }
}

uint8_t* svdb_pm_v2_read_page(svdb_page_manager_v2_t* pm, uint32_t page_num, size_t* out_size) {
    if (!pm || !out_size) return nullptr;
    try {
        return pm->ptr->ReadPage(page_num, out_size);
    } catch (...) {
        return nullptr;
    }
}

void svdb_pm_v2_write_page(svdb_page_manager_v2_t* pm, uint32_t page_num, 
                           const uint8_t* data, size_t size) {
    if (!pm || !data || size == 0) return;
    try {
        pm->ptr->WritePage(page_num, data, size);
    } catch (...) {
        // Silently ignore write errors for now
    }
}

uint32_t svdb_pm_v2_allocate_page(svdb_page_manager_v2_t* pm) {
    if (!pm) return 0;
    try {
        return pm->ptr->AllocatePage();
    } catch (...) {
        return 0;
    }
}

void svdb_pm_v2_free_page(svdb_page_manager_v2_t* pm, uint32_t page_num) {
    if (!pm) return;
    try {
        pm->ptr->FreePage(page_num);
    } catch (...) {
        // Silently ignore
    }
}

uint32_t svdb_pm_v2_get_page_size(svdb_page_manager_v2_t* pm) {
    return pm ? pm->ptr->GetPageSize() : 0;
}

uint32_t svdb_pm_v2_get_page_count(svdb_page_manager_v2_t* pm) {
    return pm ? pm->ptr->GetPageCount() : 0;
}

uint64_t svdb_pm_v2_get_file_size(svdb_page_manager_v2_t* pm) {
    return pm ? pm->ptr->GetFileSize() : 0;
}

void svdb_pm_v2_sync(svdb_page_manager_v2_t* pm) {
    if (pm) pm->ptr->Sync();
}

void svdb_pm_v2_close(svdb_page_manager_v2_t* pm) {
    if (pm) pm->ptr->Close();
}

int svdb_pm_v2_is_open(svdb_page_manager_v2_t* pm) {
    return pm ? (pm->ptr->IsOpen() ? 1 : 0) : 0;
}

void svdb_pm_v2_clear_cache(svdb_page_manager_v2_t* pm) {
    if (pm) pm->ptr->ClearCache();
}

size_t svdb_pm_v2_get_cache_size(svdb_page_manager_v2_t* pm) {
    return pm ? pm->ptr->GetCacheSize() : 0;
}

}  // extern "C"

// ============================================================================
// PageManagerV2 Implementation
// ============================================================================

PageManagerV2::PageManagerV2(const std::string& db_path,
                             uint32_t page_size,
                             bool create_if_missing)
    : db_path_(db_path)
    , page_size_(page_size)
    , page_count_(0)
    , is_open_(false)
    , mmap_enabled_(false)
    , mmap_huge_pages_(false)
    , mmap_data_(nullptr)
    , mmap_size_(0)
    , mmap_fd_(-1)
{
    // Initialize cache with 256 pages (1MB default for 4KB pages)
    cache_ = std::make_unique<LRUCacheV2>(256);
    freelist_ = std::make_unique<FreeListV2>();
    
    // Zero-initialize header
    std::memset(&header_, 0, sizeof(header_));
    
    // Try to open existing file first
    bool file_exists = OpenFile();
    
    if (!file_exists && create_if_missing) {
        // Create new file
        InitializeDatabase();
        is_open_.store(true);
    } else if (file_exists) {
        // Existing file
        if (file_.tellg() == 0 || file_.tellg() == -1) {
            // Empty file, initialize
            InitializeDatabase();
        } else {
            LoadHeader();
        }
        is_open_.store(true);
    } else if (!create_if_missing) {
        throw std::runtime_error("Database file does not exist");
    }
}

PageManagerV2::~PageManagerV2() {
    Close();
}

void PageManagerV2::InitializeDatabase() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Set up header
    header_.magic = DBHeader::MAGIC;
    header_.version = DBHeader::VERSION;
    header_.page_size = page_size_;
    header_.page_count = 1;  // First page is header
    header_.schema_version = 1;
    header_.freelist_head = 0;
    header_.freelist_count = 0;
    header_.reserved_space = 0;
    header_.write_version = 1;
    header_.read_version = 1;
    header_.schema_cookie = 0;
    header_.file_format = 1;
    
    // Write header to page 1
    std::vector<uint8_t> header_data(page_size_, 0);
    std::memcpy(header_data.data(), &header_, sizeof(header_));
    
    // Write to file
    file_.seekp(0, std::ios::beg);
    file_.write(reinterpret_cast<const char*>(header_data.data()), page_size_);
    file_.flush();
    
    page_count_.store(1);
}

void PageManagerV2::LoadHeader() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    file_.seekg(0, std::ios::beg);
    file_.read(reinterpret_cast<char*>(&header_), sizeof(header_));
    
    if (header_.magic != DBHeader::MAGIC) {
        throw std::runtime_error("Invalid database file format");
    }
    
    page_size_ = header_.page_size;
    page_count_.store(header_.page_count);
}

void PageManagerV2::SaveHeader() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    header_.page_count = page_count_.load();
    
    file_.seekp(0, std::ios::beg);
    file_.write(reinterpret_cast<const char*>(&header_), sizeof(header_));
    file_.flush();
}

bool PageManagerV2::OpenFile() {
    struct stat st;
    if (stat(db_path_.c_str(), &st) != 0) {
        // File doesn't exist
        file_.open(db_path_, std::ios::in | std::ios::out | std::ios::binary | std::ios::trunc);
        return file_.is_open();
    }
    
    file_.open(db_path_, std::ios::in | std::ios::out | std::ios::binary);
    return file_.is_open();
}

void PageManagerV2::CloseFile() {
    if (file_.is_open()) {
        file_.close();
    }
}

size_t PageManagerV2::ReadFromFile(uint64_t offset, void* buffer, size_t size) {
    std::lock_guard<std::mutex> lock(mutex_);
    file_.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
    file_.read(reinterpret_cast<char*>(buffer), static_cast<std::streamsize>(size));
    return static_cast<size_t>(file_.gcount());
}

void PageManagerV2::WriteToFile(uint64_t offset, const void* buffer, size_t size) {
    std::lock_guard<std::mutex> lock(mutex_);
    file_.seekp(static_cast<std::streamoff>(offset), std::ios::beg);
    file_.write(reinterpret_cast<const char*>(buffer), static_cast<std::streamsize>(size));
}

uint8_t* PageManagerV2::ReadPage(uint32_t page_num, size_t* out_size) {
    if (page_num == 0 || page_num > page_count_.load()) {
        if (out_size) *out_size = 0;
        return nullptr;
    }

    // Check cache first
    size_t size = 0;
    uint8_t* cached = cache_->Get(page_num, &size);
    if (cached) {
        if (out_size) *out_size = size;
        return cached;
    }

    // Try mmap first if enabled (read-only, zero-copy)
    if (mmap_enabled_ && mmap_data_ != nullptr) {
        uint64_t offset = static_cast<uint64_t>(page_num - 1) * page_size_;
        if (offset + page_size_ <= mmap_size_) {
            // Return pointer directly into mmap'd region
            // Note: Caller should NOT free this pointer!
            if (out_size) *out_size = page_size_;
            return static_cast<uint8_t*>(mmap_data_) + offset;
        }
    }

    // Read from file (fallback)
    uint8_t* buffer = new uint8_t[page_size_];
    uint64_t offset = static_cast<uint64_t>(page_num - 1) * page_size_;
    ReadFromFile(offset, buffer, page_size_);

    // Add to cache
    cache_->Put(page_num, buffer, page_size_);

    if (out_size) *out_size = page_size_;
    return buffer;
}

void PageManagerV2::FreePageBuffer(uint8_t* page) {
    delete[] page;
}

void PageManagerV2::WritePage(uint32_t page_num, const uint8_t* data, size_t size) {
    if (page_num == 0 || page_num > page_count_.load()) {
        return;
    }
    
    // Update cache
    cache_->Put(page_num, data, size);
    
    // Write to file
    uint64_t offset = static_cast<uint64_t>(page_num - 1) * page_size_;
    WriteToFile(offset, data, size);
}

uint32_t PageManagerV2::AllocatePage() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Check freelist first
    uint32_t page_num = freelist_->Allocate();
    if (page_num != 0) {
        return page_num;
    }
    
    // Allocate new page
    page_num = page_count_.load() + 1;
    page_count_.store(page_num);
    
    // Update header
    SaveHeader();
    
    return page_num;
}

void PageManagerV2::FreePage(uint32_t page_num) {
    if (page_num == 0 || page_num > page_count_.load()) {
        return;
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    freelist_->Add(page_num);
    
    // Remove from cache
    cache_->Remove(page_num);
}

uint64_t PageManagerV2::GetFileSize() const {
    struct stat st;
    if (stat(db_path_.c_str(), &st) != 0) {
        return 0;
    }
    return static_cast<uint64_t>(st.st_size);
}

void PageManagerV2::Sync() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (file_.is_open()) {
        file_.flush();
    }
}

void PageManagerV2::Checkpoint() {
    Sync();
    // In v0.11.3, checkpoint is a no-op for in-memory databases
    // For WAL mode, this would merge WAL into main file
}

void PageManagerV2::ClearCache() {
    cache_->Clear();
}

size_t PageManagerV2::GetCacheSize() const {
    return cache_->Size();
}

void PageManagerV2::Close() {
    if (!is_open_.exchange(false)) {
        return;
    }

    Sync();
    SaveHeader();

    // Clean up mmap if enabled
    if (mmap_data_ != nullptr && mmap_data_ != MAP_FAILED) {
        munmap(mmap_data_, mmap_size_);
        mmap_data_ = nullptr;
    }
    if (mmap_fd_ >= 0) {
        close(mmap_fd_);
        mmap_fd_ = -1;
    }
    mmap_size_ = 0;
    mmap_enabled_ = false;

    CloseFile();

    cache_->Clear();
    freelist_->Clear();
}

DBHeader PageManagerV2::GetHeader() const {
    return header_;
}

void PageManagerV2::UpdateHeader(const DBHeader& header) {
    std::lock_guard<std::mutex> lock(mutex_);
    header_ = header;
    SaveHeader();
}

// ============================================================================
// WS3: Memory-Mapped I/O Implementation
// ============================================================================

void PageManagerV2::EnableMMap(bool enable, bool use_huge_pages) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (enable == mmap_enabled_) {
        return;  // No change
    }

    if (enable) {
        // Enable mmap
        if (!file_.is_open()) {
            return;
        }

        // Close fstream and open with separate fd for mmap
        file_.flush();
        file_.close();

        // Open with O_RDONLY for mmap
        int flags = O_RDONLY;
#ifdef MAP_HUGETLB
        if (use_huge_pages) {
            flags = O_RDONLY | O_LARGEFILE;
        }
#endif

        mmap_fd_ = open(db_path_.c_str(), flags);
        if (mmap_fd_ < 0) {
            // Reopen fstream on failure
            file_.open(db_path_, std::ios::in | std::ios::out | std::ios::binary);
            return;
        }

        // Get file size
        struct stat st;
        if (fstat(mmap_fd_, &st) != 0) {
            close(mmap_fd_);
            mmap_fd_ = -1;
            file_.open(db_path_, std::ios::in | std::ios::out | std::ios::binary);
            return;
        }

        mmap_size_ = static_cast<size_t>(st.st_size);

        if (mmap_size_ > 0) {
            int prot = PROT_READ;
            int map_flags = MAP_SHARED;

#ifdef MAP_HUGETLB
            if (use_huge_pages && mmap_size_ >= (2 * 1024 * 1024)) {
                map_flags |= MAP_HUGETLB;
            }
#endif

            mmap_data_ = mmap(nullptr, mmap_size_, prot, map_flags, mmap_fd_, 0);

            if (mmap_data_ == MAP_FAILED) {
                mmap_data_ = nullptr;
                close(mmap_fd_);
                mmap_fd_ = -1;
                mmap_size_ = 0;
                file_.open(db_path_, std::ios::in | std::ios::out | std::ios::binary);
                return;
            }

            mmap_enabled_ = true;
            mmap_huge_pages_ = use_huge_pages;
        }
    } else {
        // Disable mmap
        if (mmap_data_ != nullptr && mmap_data_ != MAP_FAILED) {
            munmap(mmap_data_, mmap_size_);
        }
        if (mmap_fd_ >= 0) {
            close(mmap_fd_);
        }

        mmap_data_ = nullptr;
        mmap_size_ = 0;
        mmap_fd_ = -1;
        mmap_enabled_ = false;
        mmap_huge_pages_ = false;

        // Reopen fstream
        file_.open(db_path_, std::ios::in | std::ios::out | std::ios::binary);
    }
}

const uint8_t* PageManagerV2::GetMMapPage(uint32_t page_num) {
    if (!mmap_enabled_ || mmap_data_ == nullptr) {
        return nullptr;
    }

    if (page_num == 0 || page_num > page_count_.load()) {
        return nullptr;
    }

    uint64_t offset = static_cast<uint64_t>(page_num - 1) * page_size_;
    if (offset + page_size_ > mmap_size_) {
        return nullptr;
    }

    return static_cast<const uint8_t*>(mmap_data_) + offset;
}

void PageManagerV2::PrefetchPages(uint32_t start_page, uint32_t count) {
    if (!mmap_enabled_ || mmap_data_ == nullptr) {
        return;
    }

    uint64_t offset = static_cast<uint64_t>(start_page - 1) * page_size_;
    uint64_t len = static_cast<uint64_t>(count) * page_size_;

    if (offset + len > mmap_size_) {
        len = mmap_size_ - offset;
    }

    if (len > 0) {
#ifdef __linux__
        madvise(static_cast<char*>(mmap_data_) + offset, len, MADV_WILLNEED);
        // Also trigger read-ahead by touching first byte of each page
        volatile char* p = static_cast<volatile char*>(mmap_data_) + offset;
        for (uint32_t i = 0; i < count && offset + i * page_size_ < mmap_size_; i++) {
            (void)p[i * page_size_];
        }
#endif
    }
}

void PageManagerV2::AdviseSequential() {
    if (mmap_enabled_ && mmap_data_ != nullptr) {
#ifdef __linux__
        madvise(mmap_data_, mmap_size_, MADV_SEQUENTIAL);
#endif
    }
}

void PageManagerV2::AdviseRandom() {
    if (mmap_enabled_ && mmap_data_ != nullptr) {
#ifdef __linux__
        madvise(mmap_data_, mmap_size_, MADV_RANDOM);
#endif
    }
}

void PageManagerV2::AdviseWillNeed(uint32_t start_page, uint32_t count) {
    PrefetchPages(start_page, count);
}

void PageManagerV2::AdviseDontNeed(uint32_t start_page, uint32_t count) {
    if (!mmap_enabled_ || mmap_data_ == nullptr) {
        return;
    }

    uint64_t offset = static_cast<uint64_t>(start_page - 1) * page_size_;
    uint64_t len = static_cast<uint64_t>(count) * page_size_;

    if (offset + len > mmap_size_) {
        len = mmap_size_ - offset;
    }

    if (len > 0) {
#ifdef __linux__
        madvise(static_cast<char*>(mmap_data_) + offset, len, MADV_DONTNEED);
#endif
    }
}

}  // namespace svdb::ds
