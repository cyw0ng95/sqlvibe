#include "manager.h"
#include "cache.h"
#include "../PB/vfs.h"
#include <string.h>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <vector>

static const char SVDB_MAGIC[17] = "SQLite format 3\0";

/* -------------------------------------------------------------------------
 * Internal helpers
 * ----------------------------------------------------------------------- */

static inline uint16_t rd16(const uint8_t* p) {
    return (uint16_t)((p[0] << 8) | p[1]);
}

static inline void wr16(uint8_t* p, uint16_t v) {
    p[0] = (uint8_t)(v >> 8);
    p[1] = (uint8_t)(v);
}

static inline uint32_t rd32(const uint8_t* p) {
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] <<  8) |  (uint32_t)p[3];
}

static inline void wr32(uint8_t* p, uint32_t v) {
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >>  8);
    p[3] = (uint8_t)(v);
}

/* -------------------------------------------------------------------------
 * Self-contained C++ PageManager
 * ----------------------------------------------------------------------- */

struct svdb_page_manager {
    std::string db_path;
    uint32_t page_size;
    uint32_t num_pages;
    void* vfs;
    void* file;
    svdb_cache_t* cache;
    std::mutex mu;
    bool is_valid;
};

static const int DEFAULT_CACHE_PAGES = 2000;

extern "C" {

svdb_page_manager* svdb_page_manager_create(const char* db_path, uint32_t page_size, int cache_pages) {
    if (!db_path || !svdb_manager_is_valid_page_size(page_size)) {
        return nullptr;
    }

    svdb_page_manager* pm = (svdb_page_manager*)std::malloc(sizeof(svdb_page_manager));
    if (!pm) return nullptr;

    pm->db_path = db_path;
    pm->page_size = page_size;
    pm->num_pages = 0;
    pm->is_valid = false;

    pm->vfs = SVDB_PB_VFS_Create();
    if (!pm->vfs) {
        std::free(pm);
        return nullptr;
    }

    int flags = static_cast<int>(svdb::pb::OpenFlags::ReadWrite) | 
                static_cast<int>(svdb::pb::OpenFlags::Create);
    pm->file = SVDB_PB_VFS_Open(pm->vfs, db_path, flags);
    if (!pm->file) {
        SVDB_PB_VFS_Destroy(pm->vfs);
        std::free(pm);
        return nullptr;
    }

    int cache_cap = (cache_pages > 0) ? cache_pages : DEFAULT_CACHE_PAGES;
    pm->cache = svdb_cache_create(cache_cap);
    if (!pm->cache) {
        SVDB_PB_VFS_Close(pm->file);
        SVDB_PB_VFS_Destroy(pm->vfs);
        std::free(pm);
        return nullptr;
    }

    std::vector<uint8_t> header(pm->page_size);
    int64_t bytes_read = SVDB_PB_VFS_Read(pm->file, header.data(), pm->page_size, 0);
    if (bytes_read > 0 && svdb_manager_header_magic_valid(header.data(), header.size())) {
        pm->num_pages = svdb_manager_read_header_num_pages(header.data(), header.size());
        if (pm->num_pages == 0) {
            int64_t file_size = SVDB_PB_VFS_GetSize(pm->file);
            if (file_size > 0) {
                pm->num_pages = (uint32_t)(file_size / pm->page_size);
            }
        }
    } else if (bytes_read < 0) {
        pm->num_pages = 0;
    }

    pm->is_valid = true;
    return pm;
}

void svdb_page_manager_destroy(svdb_page_manager* pm) {
    if (!pm) return;

    if (pm->cache) svdb_cache_destroy(pm->cache);
    if (pm->file) SVDB_PB_VFS_Close(pm->file);
    if (pm->vfs) SVDB_PB_VFS_Destroy(pm->vfs);

    std::free(pm);
}

int svdb_page_manager_read(svdb_page_manager* pm, uint32_t page_num,
                           const uint8_t** page_data, size_t* page_size) {
    if (!pm || !pm->is_valid || page_num == 0) return 0;

    std::lock_guard<std::mutex> lock(pm->mu);

    if (svdb_cache_get(pm->cache, page_num, page_data, page_size)) {
        return 1;
    }

    int64_t offset = svdb_manager_page_offset(page_num, pm->page_size);
    if (offset < 0) return 0;

    std::vector<uint8_t> buffer(pm->page_size);
    int64_t bytes_read = SVDB_PB_VFS_Read(pm->file, buffer.data(), pm->page_size, offset);
    if (bytes_read != (int64_t)pm->page_size) {
        return 0;
    }

    svdb_cache_set(pm->cache, page_num, buffer.data(), pm->page_size);
    return svdb_cache_get(pm->cache, page_num, page_data, page_size);
}

int svdb_page_manager_write(svdb_page_manager* pm, uint32_t page_num,
                            const uint8_t* page_data, size_t page_size) {
    if (!pm || !pm->is_valid || page_num == 0 || !page_data) return 0;
    if (page_size != pm->page_size) return 0;

    std::lock_guard<std::mutex> lock(pm->mu);

    svdb_cache_set(pm->cache, page_num, page_data, page_size);

    int64_t offset = svdb_manager_page_offset(page_num, pm->page_size);
    if (offset < 0) return 0;

    int64_t bytes_written = SVDB_PB_VFS_Write(pm->file, page_data, page_size, offset);
    if (bytes_written != (int64_t)page_size) {
        return 0;
    }

    if (page_num > pm->num_pages) {
        pm->num_pages = page_num;
    }

    return 1;
}

int svdb_page_manager_allocate(svdb_page_manager* pm, uint32_t* page_num) {
    if (!pm || !pm->is_valid || !page_num) return 0;

    std::lock_guard<std::mutex> lock(pm->mu);

    pm->num_pages++;
    *page_num = pm->num_pages;

    int64_t new_size = (int64_t)pm->num_pages * pm->page_size;
    int64_t current_size = SVDB_PB_VFS_GetSize(pm->file);
    
    if (current_size < new_size) {
        std::vector<uint8_t> zero_page(pm->page_size, 0);
        int64_t offset = svdb_manager_page_offset(*page_num, pm->page_size);
        SVDB_PB_VFS_Write(pm->file, zero_page.data(), pm->page_size, offset);
    }

    return 1;
}

int svdb_page_manager_free(svdb_page_manager* pm, uint32_t page_num) {
    if (!pm || !pm->is_valid || page_num == 0) return 0;

    std::lock_guard<std::mutex> lock(pm->mu);

    svdb_cache_remove(pm->cache, page_num);

    return 1;
}

uint32_t svdb_page_manager_get_page_size(const svdb_page_manager* pm) {
    if (!pm) return 0;
    return pm->page_size;
}

uint32_t svdb_page_manager_get_num_pages(const svdb_page_manager* pm) {
    if (!pm) return 0;
    return pm->num_pages;
}

int svdb_page_manager_sync(svdb_page_manager* pm) {
    if (!pm || !pm->is_valid) return 0;

    std::lock_guard<std::mutex> lock(pm->mu);

    int result = SVDB_PB_VFS_Sync(pm->file);

    if (result == 0) {
        std::vector<uint8_t> header(pm->page_size, 0);
        std::memcpy(header.data(), SVDB_MAGIC, 16);
        svdb_manager_write_header_page_size(header.data(), header.size(), pm->page_size);
        svdb_manager_write_header_num_pages(header.data(), header.size(), pm->num_pages);
        SVDB_PB_VFS_Write(pm->file, header.data(), pm->page_size, 0);
        SVDB_PB_VFS_Sync(pm->file);
    }

    return result == 0 ? 1 : 0;
}

} /* extern "C" */

/* -------------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

int64_t svdb_manager_page_offset(uint32_t page_num, uint32_t page_size) {
    if (page_num == 0) return -1;
    return (int64_t)(page_num - 1) * (int64_t)page_size;
}

int svdb_manager_is_valid_page_size(uint32_t page_size) {
    if (page_size < SVDB_MANAGER_MIN_PAGE_SIZE) return 0;
    if (page_size > SVDB_MANAGER_MAX_PAGE_SIZE) return 0;
    return (page_size & (page_size - 1)) == 0;
}

int svdb_manager_header_magic_valid(const uint8_t* data, size_t data_size) {
    if (!data || data_size < 16) return 0;
    return memcmp(data, SVDB_MAGIC, 16) == 0;
}

uint32_t svdb_manager_read_header_page_size(const uint8_t* data, size_t data_size) {
    if (!data || data_size < 18) return 0;
    uint16_t v = rd16(data + 16);
    return (v == 1) ? 65536u : (uint32_t)v;
}

int svdb_manager_write_header_page_size(uint8_t* data, size_t data_size, uint32_t page_size) {
    if (!data || data_size < 18) return 0;
    if (!svdb_manager_is_valid_page_size(page_size)) return 0;
    uint16_t stored = (page_size == 65536) ? 1 : (uint16_t)page_size;
    wr16(data + 16, stored);
    return 1;
}

uint32_t svdb_manager_read_header_num_pages(const uint8_t* data, size_t data_size) {
    if (!data || data_size < 32) return 0;
    return rd32(data + 28);
}

int svdb_manager_write_header_num_pages(uint8_t* data, size_t data_size, uint32_t num_pages) {
    if (!data || data_size < 32) return 0;
    wr32(data + 28, num_pages);
    return 1;
}
