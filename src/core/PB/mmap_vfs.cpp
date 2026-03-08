/* mmap_vfs.cpp — Memory-mapped I/O VFS implementation */
#include "mmap_vfs.h"
#include "../SF/svdb_assert.h"

#include <cstring>
#include <cstdlib>
#include <vector>
#include <mutex>
#include <algorithm>
#include <string>

#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>

extern "C" {

struct svdb_mmap_file_s {
    int fd;
    int64_t size;
    void* mapped_data;
    int64_t mapped_size;
    int use_huge_pages;
    std::string path;

    svdb_mmap_file_s() : fd(-1), size(0), mapped_data(nullptr),
                          mapped_size(0), use_huge_pages(0) {}
};

struct svdb_mmap_pool_s {
    std::vector<svdb_mmap_file_t*> files;
    std::mutex mutex;
    int max_regions;

    svdb_mmap_pool_s(int max) : max_regions(max) {}
};

/* ============================================================================
 * MMap file operations
 * ============================================================================ */

svdb_mmap_file_t* svdb_mmap_file_open(const char* path, int use_huge_pages) {
    if (!path || path[0] == '\0') {
        return nullptr;
    }

    /* Open file read-only */
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        return nullptr;
    }

    /* Get file size */
    struct stat st;
    if (fstat(fd, &st) != 0) {
        close(fd);
        return nullptr;
    }

    int64_t size = st.st_size;
    if (size == 0) {
        /* Empty file - don't map, but return valid handle */
        svdb_mmap_file_t* file = new (std::nothrow) svdb_mmap_file_t();
        if (!file) {
            close(fd);
            return nullptr;
        }
        file->fd = fd;
        file->size = 0;
        file->mapped_data = nullptr;
        file->mapped_size = 0;
        file->use_huge_pages = 0;
        file->path = path;
        return file;
    }

    /* Determine map flags */
    int mmap_flags = MAP_SHARED | MAP_POPULATE;
    if (use_huge_pages && size >= SVDB_MMAP_HUGE_PAGE_SIZE) {
#ifdef MAP_HUGETLB
        /* Try huge pages, fall back to regular pages if not available */
        mmap_flags |= MAP_HUGETLB;
#endif
    }

    /* Map the entire file */
    void* mapped = mmap(nullptr, size, PROT_READ, mmap_flags, fd, 0);
    if (mapped == MAP_FAILED) {
        /* Try again without huge pages */
        if (use_huge_pages) {
            mmap_flags &= ~(MAP_HUGETLB);
            mapped = mmap(nullptr, size, PROT_READ, mmap_flags, fd, 0);
        }

        if (mapped == MAP_FAILED) {
            close(fd);
            return nullptr;
        }
    }

    svdb_mmap_file_t* file = new (std::nothrow) svdb_mmap_file_t();
    if (!file) {
        munmap(mapped, size);
        close(fd);
        return nullptr;
    }

    file->fd = fd;
    file->size = size;
    file->mapped_data = mapped;
    file->mapped_size = size;
    file->use_huge_pages = use_huge_pages;
    file->path = path;

    return file;
}

void svdb_mmap_file_close(svdb_mmap_file_t* file) {
    if (!file) return;

    if (file->mapped_data && file->mapped_size > 0) {
        munmap(file->mapped_data, file->mapped_size);
    }

    if (file->fd >= 0) {
        close(file->fd);
    }

    delete file;
}

const void* svdb_mmap_file_get_ptr(svdb_mmap_file_t* file, int64_t offset) {
    if (!file || offset < 0 || offset >= file->size || !file->mapped_data) {
        return nullptr;
    }

    return static_cast<const char*>(file->mapped_data) + offset;
}

const void* svdb_mmap_file_get_range(svdb_mmap_file_t* file,
                                      int64_t offset, int64_t len) {
    if (!file || !file->mapped_data) return nullptr;
    if (offset < 0 || len < 0) return nullptr;
    if (offset + len > file->size) return nullptr;

    return static_cast<const char*>(file->mapped_data) + offset;
}

int64_t svdb_mmap_file_read(svdb_mmap_file_t* file,
                             void* buffer,
                             int64_t len,
                             int64_t offset) {
    if (!file || !buffer) return -1;
    if (offset < 0 || len < 0) return -1;
    if (!file->mapped_data) return -1;

    /* Clamp to available range */
    if (offset >= file->size) return 0;

    int64_t available = file->size - offset;
    int64_t to_read = (len < available) ? len : available;

    memcpy(buffer, static_cast<const char*>(file->mapped_data) + offset, to_read);
    return to_read;
}

int64_t svdb_mmap_file_size(svdb_mmap_file_t* file) {
    return file ? file->size : -1;
}

int svdb_mmap_file_is_valid(svdb_mmap_file_t* file) {
    if (!file) return 0;
    if (file->fd < 0) return 0;
    if (file->size > 0 && !file->mapped_data) return 0;
    return 1;
}

int svdb_mmap_file_fd(svdb_mmap_file_t* file) {
    return file ? file->fd : -1;
}

int svdb_mmap_file_set_access(svdb_mmap_file_t* file,
                               svdb_mmap_access_t access,
                               int64_t offset,
                               int64_t len) {
    if (!file || !file->mapped_data) return -1;
    if (offset < 0 || len < 0) return -1;

    /* Clamp to mapped range */
    if (offset >= file->mapped_size) return 0;
    if (offset + len > file->mapped_size) {
        len = file->mapped_size - offset;
    }

    int advice;
    switch (access) {
        case SVDB_MMAP_ACCESS_RANDOM:
            advice = MADV_RANDOM;
            break;
        case SVDB_MMAP_ACCESS_SEQUENTIAL:
            advice = MADV_SEQUENTIAL;
            break;
        case SVDB_MMAP_ACCESS_WILLNEED:
            advice = MADV_WILLNEED;
            break;
        case SVDB_MMAP_ACCESS_DONTNEED:
            advice = MADV_DONTNEED;
            break;
        default:
            return -1;
    }

    void* addr = static_cast<char*>(file->mapped_data) + offset;
    return madvise(addr, len, advice);
}

int svdb_mmap_file_prefetch(svdb_mmap_file_t* file,
                             int64_t offset,
                             int64_t len) {
    return svdb_mmap_file_set_access(file, SVDB_MMAP_ACCESS_WILLNEED, offset, len);
}

int svdb_mmap_file_release(svdb_mmap_file_t* file,
                            int64_t offset,
                            int64_t len) {
    return svdb_mmap_file_set_access(file, SVDB_MMAP_ACCESS_DONTNEED, offset, len);
}

int svdb_mmap_file_remap(svdb_mmap_file_t* file) {
    if (!file || file->fd < 0) return -1;

    /* Get new size */
    struct stat st;
    if (fstat(file->fd, &st) != 0) {
        return -1;
    }

    int64_t new_size = st.st_size;

    /* If size unchanged, nothing to do */
    if (new_size == file->mapped_size) {
        return 0;
    }

    /* Unmap old region */
    if (file->mapped_data && file->mapped_size > 0) {
        munmap(file->mapped_data, file->mapped_size);
        file->mapped_data = nullptr;
        file->mapped_size = 0;
    }

    /* Map new region if non-empty */
    if (new_size > 0) {
        int mmap_flags = MAP_SHARED | MAP_POPULATE;
        if (file->use_huge_pages && new_size >= SVDB_MMAP_HUGE_PAGE_SIZE) {
#ifdef MAP_HUGETLB
            mmap_flags |= MAP_HUGETLB;
#endif
        }

        void* mapped = mmap(nullptr, new_size, PROT_READ, mmap_flags, file->fd, 0);
        if (mapped == MAP_FAILED) {
            if (file->use_huge_pages) {
                mmap_flags &= ~(MAP_HUGETLB);
                mapped = mmap(nullptr, new_size, PROT_READ, mmap_flags, file->fd, 0);
            }

            if (mapped == MAP_FAILED) {
                file->size = new_size;
                return -1;
            }
        }

        file->mapped_data = mapped;
        file->mapped_size = new_size;
    }

    file->size = new_size;
    return 0;
}

int svdb_mmap_file_sync(svdb_mmap_file_t* file) {
    if (!file || !file->mapped_data) return -1;

    /* msync with MS_SYNC for synchronous sync */
    return msync(file->mapped_data, file->mapped_size, MS_SYNC);
}

/* ============================================================================
 * MMap pool operations
 * ============================================================================ */

svdb_mmap_pool_t* svdb_mmap_pool_create(int max_regions) {
    if (max_regions <= 0) {
        max_regions = SVDB_MMAP_MAX_REGIONS;
    }

    try {
        return new svdb_mmap_pool_t(max_regions);
    } catch (...) {
        return nullptr;
    }
}

void svdb_mmap_pool_destroy(svdb_mmap_pool_t* pool) {
    if (!pool) return;

    std::lock_guard<std::mutex> lock(pool->mutex);

    for (auto* file : pool->files) {
        svdb_mmap_file_close(file);
    }

    delete pool;
}

svdb_mmap_file_t* svdb_mmap_pool_open(svdb_mmap_pool_t* pool,
                                       const char* path,
                                       int use_huge_pages) {
    if (!pool || !path) return nullptr;

    std::lock_guard<std::mutex> lock(pool->mutex);

    /* Check if we've hit the limit */
    if (static_cast<int>(pool->files.size()) >= pool->max_regions) {
        return nullptr;
    }

    svdb_mmap_file_t* file = svdb_mmap_file_open(path, use_huge_pages);
    if (!file) return nullptr;

    pool->files.push_back(file);
    return file;
}

void svdb_mmap_pool_close(svdb_mmap_pool_t* pool, svdb_mmap_file_t* file) {
    if (!pool || !file) return;

    std::lock_guard<std::mutex> lock(pool->mutex);

    auto it = std::find(pool->files.begin(), pool->files.end(), file);
    if (it != pool->files.end()) {
        pool->files.erase(it);
        svdb_mmap_file_close(file);
    }
}

int64_t svdb_mmap_pool_total_memory(svdb_mmap_pool_t* pool) {
    if (!pool) return 0;

    std::lock_guard<std::mutex> lock(pool->mutex);

    int64_t total = 0;
    for (const auto* file : pool->files) {
        total += file->mapped_size;
    }
    return total;
}

int svdb_mmap_pool_mapping_count(svdb_mmap_pool_t* pool) {
    if (!pool) return 0;

    std::lock_guard<std::mutex> lock(pool->mutex);
    return static_cast<int>(pool->files.size());
}

/* ============================================================================
 * Utility functions
 * ============================================================================ */

int svdb_mmap_huge_pages_available(void) {
#ifdef MAP_HUGETLB
    /* Try to map a single huge page to test availability */
    int fd = open("/dev/zero", O_RDONLY);
    if (fd < 0) return 0;

    void* p = mmap(nullptr, SVDB_MMAP_HUGE_PAGE_SIZE, PROT_READ,
                   MAP_PRIVATE | MAP_HUGETLB, fd, 0);
    close(fd);

    if (p == MAP_FAILED) {
        return 0;
    }

    munmap(p, SVDB_MMAP_HUGE_PAGE_SIZE);
    return 1;
#else
    return 0;
#endif
}

int64_t svdb_mmap_page_size(void) {
    static int64_t page_size = 0;
    if (page_size == 0) {
        page_size = sysconf(_SC_PAGESIZE);
    }
    return page_size;
}

int64_t svdb_mmap_page_align_up(int64_t offset) {
    int64_t ps = svdb_mmap_page_size();
    return (offset + ps - 1) & ~(ps - 1);
}

int64_t svdb_mmap_page_align_down(int64_t offset) {
    int64_t ps = svdb_mmap_page_size();
    return offset & ~(ps - 1);
}

} // extern "C"