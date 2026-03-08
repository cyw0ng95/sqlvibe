/* io_uring.cpp — io_uring based async I/O implementation for Linux */
#include "io_uring.h"
#include "../SF/svdb_assert.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <cstring>
#include <cstdlib>
#include <atomic>
#include <vector>
#include <mutex>
#include <string>

#ifdef __linux__
#include <sys/syscall.h>
#include <sys/mman.h>

/* Define syscall numbers if not available */
#ifndef __NR_io_uring_setup
#define __NR_io_uring_setup 425
#endif
#ifndef __NR_io_uring_enter
#define __NR_io_uring_enter 426
#endif
#ifndef __NR_io_uring_register
#define __NR_io_uring_register 427
#endif

/* io_uring opcodes */
#ifndef IORING_OP_READV
#define IORING_OP_READV     7
#endif
#ifndef IORING_OP_WRITEV
#define IORING_OP_WRITEV    8
#endif
#ifndef IORING_OP_FSYNC
#define IORING_OP_FSYNC     9
#endif
#ifndef IORING_OP_READ
#define IORING_OP_READ      22
#endif
#ifndef IORING_OP_WRITE
#define IORING_OP_WRITE     23
#endif

/* io_uring setup flags */
#ifndef IORING_SETUP_SQPOLL
#define IORING_SETUP_SQPOLL (1U << 1)
#endif

/* sqe flags */
#ifndef IOSQE_IO_LINK
#define IOSQE_IO_LINK       (1U << 2)
#endif

/* Offsets for mmap */
#ifndef IORING_OFF_SQ_RING
#define IORING_OFF_SQ_RING  0
#endif
#ifndef IORING_OFF_CQ_RING
#define IORING_OFF_CQ_RING  0x8000000ULL
#endif
#ifndef IORING_OFF_SQES
#define IORING_OFF_SQES     0x10000000ULL
#endif

#endif // __linux__

extern "C" {

/* Fallback for non-Linux systems */
#ifndef __linux__
struct svdb_io_uring_s { int dummy; };
struct svdb_io_file_s { int dummy; };
struct svdb_io_batch_s { int dummy; };

svdb_io_uring_t* svdb_io_uring_create(uint32_t, int) { return nullptr; }
void svdb_io_uring_destroy(svdb_io_uring_t*) {}
int svdb_io_uring_submit(svdb_io_uring_t*) { return -1; }
int svdb_io_uring_submit_and_wait(svdb_io_uring_t*, uint32_t) { return -1; }
int svdb_io_uring_reap(svdb_io_uring_t*, void (*)(uint64_t, int, void*), void*) { return 0; }
int svdb_io_uring_available(void) { return 0; }
svdb_io_file_t* svdb_io_uring_open_file(svdb_io_uring_t*, const char*, int, int) { return nullptr; }
void svdb_io_uring_close_file(svdb_io_file_t*) {}
int svdb_io_uring_file_fd(svdb_io_file_t*) { return -1; }
int64_t svdb_io_uring_file_size(svdb_io_file_t*) { return -1; }
int svdb_io_uring_read(svdb_io_uring_t*, svdb_io_file_t*, void*, size_t, int64_t, uint64_t) { return -1; }
int svdb_io_uring_write(svdb_io_uring_t*, svdb_io_file_t*, const void*, size_t, int64_t, uint64_t) { return -1; }
int svdb_io_uring_readv(svdb_io_uring_t*, svdb_io_file_t*, const iovec*, size_t, int64_t, uint64_t) { return -1; }
int svdb_io_uring_writev(svdb_io_uring_t*, svdb_io_file_t*, const iovec*, size_t, int64_t, uint64_t) { return -1; }
int svdb_io_uring_fsync(svdb_io_uring_t*, svdb_io_file_t*, uint64_t) { return -1; }
svdb_io_batch_t* svdb_io_batch_create(svdb_io_uring_t*) { return nullptr; }
void svdb_io_batch_destroy(svdb_io_batch_t*) {}
int svdb_io_batch_add_write(svdb_io_batch_t*, svdb_io_file_t*, const void*, size_t, int64_t, uint64_t) { return -1; }
int svdb_io_batch_submit(svdb_io_batch_t*, int) { return -1; }
void svdb_io_batch_clear(svdb_io_batch_t*) {}
size_t svdb_io_batch_count(svdb_io_batch_t*) { return 0; }
int64_t svdb_io_uring_read_sync(svdb_io_uring_t*, svdb_io_file_t*, void*, size_t, int64_t) { return -1; }
int64_t svdb_io_uring_write_sync(svdb_io_uring_t*, svdb_io_file_t*, const void*, size_t, int64_t) { return -1; }

#else // __linux__

/* Use kernel io_uring structures directly via syscall interface */
struct svdb_io_uring_params {
    uint32_t sq_entries;
    uint32_t cq_entries;
    uint32_t flags;
    uint32_t sq_thread_cpu;
    uint32_t sq_thread_idle;
    uint32_t features;
    uint32_t resv[4];
    struct {
        uint32_t head;
        uint32_t tail;
        uint32_t ring_mask;
        uint32_t ring_entries;
        uint32_t flags;
        uint32_t dropped;
        uint32_t array;
        uint32_t resv1;
        uint64_t resv2;
    } sq_off;
    struct {
        uint32_t head;
        uint32_t tail;
        uint32_t ring_mask;
        uint32_t ring_entries;
        uint32_t overflow;
        uint32_t cqes;
        uint64_t resv[2];
    } cq_off;
};

struct svdb_io_uring_sqe {
    uint8_t opcode;
    uint8_t flags;
    uint16_t ioprio;
    int32_t fd;
    uint64_t off;
    uint64_t addr;
    uint32_t len;
    uint32_t rw_flags;
    uint64_t user_data;
    uint16_t buf_index;
    uint16_t pad[3];
    uint64_t pad2;
};

struct svdb_io_uring_cqe {
    uint64_t user_data;
    int32_t res;
    uint32_t flags;
};

struct svdb_io_uring_s {
    int ring_fd;
    uint32_t sq_ring_mask;
    uint32_t cq_ring_mask;
    uint32_t sq_entries;
    uint32_t cq_entries;

    /* Submission queue */
    uint32_t* sq_head;
    uint32_t* sq_tail;
    uint32_t* sq_array;
    svdb_io_uring_sqe* sqes;

    /* Completion queue */
    uint32_t* cq_head;
    uint32_t* cq_tail;
    svdb_io_uring_cqe* cqes;

    /* State */
    std::atomic<uint32_t> sq_tail_cached;

    svdb_io_uring_s() : ring_fd(-1), sq_ring_mask(0), cq_ring_mask(0),
                         sq_entries(0), cq_entries(0),
                         sq_head(nullptr), sq_tail(nullptr), sq_array(nullptr), sqes(nullptr),
                         cq_head(nullptr), cq_tail(nullptr), cqes(nullptr),
                         sq_tail_cached(0) {}
};

struct svdb_io_file_s {
    int fd;
    std::string path;
    int64_t size;
    bool writable;

    svdb_io_file_s() : fd(-1), size(0), writable(false) {}
};

struct svdb_io_batch_s {
    svdb_io_uring_t* ring;
    struct BatchEntry {
        svdb_io_file_t* file;
        std::vector<uint8_t> data;
        int64_t offset;
        uint64_t tag;
    };
    std::vector<BatchEntry> entries;

    svdb_io_batch_s(svdb_io_uring_t* r) : ring(r) {}
};

/* ============================================================================
 * Syscall wrappers
 * ============================================================================ */

static int sys_io_uring_setup(uint32_t entries, svdb_io_uring_params* params) {
    return static_cast<int>(syscall(__NR_io_uring_setup, entries, params));
}

static int sys_io_uring_enter(int fd, uint32_t to_submit, uint32_t min_complete,
                               uint32_t flags, sigset_t* sig) {
    return static_cast<int>(syscall(__NR_io_uring_enter, fd, to_submit, min_complete, flags, sig));
}

/* ============================================================================
 * io_uring instance management
 * ============================================================================ */

static int io_uring_mmap(int fd, svdb_io_uring_params* params, svdb_io_uring_t* ring) {
    size_t sq_ring_size = params->sq_off.array + params->sq_entries * sizeof(uint32_t);
    size_t cq_ring_size = params->cq_off.cqes + params->cq_entries * sizeof(svdb_io_uring_cqe);
    size_t sqes_size = params->sq_entries * sizeof(svdb_io_uring_sqe);

    void* sq_ring = mmap(nullptr, sq_ring_size, PROT_READ | PROT_WRITE,
                         MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_SQ_RING);
    if (sq_ring == MAP_FAILED) return -1;

    void* cq_ring = mmap(nullptr, cq_ring_size, PROT_READ | PROT_WRITE,
                         MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_CQ_RING);
    if (cq_ring == MAP_FAILED) {
        munmap(sq_ring, sq_ring_size);
        return -1;
    }

    void* sqes = mmap(nullptr, sqes_size, PROT_READ | PROT_WRITE,
                      MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_SQES);
    if (sqes == MAP_FAILED) {
        munmap(sq_ring, sq_ring_size);
        munmap(cq_ring, cq_ring_size);
        return -1;
    }

    ring->sq_head = (uint32_t*)((char*)sq_ring + params->sq_off.head);
    ring->sq_tail = (uint32_t*)((char*)sq_ring + params->sq_off.tail);
    ring->sq_ring_mask = *(uint32_t*)((char*)sq_ring + params->sq_off.ring_mask);
    ring->sq_entries = *(uint32_t*)((char*)sq_ring + params->sq_off.ring_entries);
    ring->sq_array = (uint32_t*)((char*)sq_ring + params->sq_off.array);
    ring->sqes = (svdb_io_uring_sqe*)sqes;

    ring->cq_head = (uint32_t*)((char*)cq_ring + params->cq_off.head);
    ring->cq_tail = (uint32_t*)((char*)cq_ring + params->cq_off.tail);
    ring->cq_ring_mask = *(uint32_t*)((char*)cq_ring + params->cq_off.ring_mask);
    ring->cq_entries = *(uint32_t*)((char*)cq_ring + params->cq_off.ring_entries);
    ring->cqes = (svdb_io_uring_cqe*)((char*)cq_ring + params->cq_off.cqes);

    return 0;
}

svdb_io_uring_t* svdb_io_uring_create(uint32_t queue_depth, int use_sqpoll) {
    if (queue_depth == 0) queue_depth = SVDB_IO_URING_DEFAULT_QUEUE_DEPTH;

    svdb_io_uring_params params = {};
    if (use_sqpoll) {
        params.flags |= IORING_SETUP_SQPOLL;
    }

    int fd = sys_io_uring_setup(queue_depth, &params);
    if (fd < 0) {
        return nullptr;
    }

    svdb_io_uring_t* ring = new (std::nothrow) svdb_io_uring_t();
    if (!ring) {
        close(fd);
        return nullptr;
    }

    if (io_uring_mmap(fd, &params, ring) != 0) {
        delete ring;
        close(fd);
        return nullptr;
    }

    ring->ring_fd = fd;

    /* Initialize sq_array */
    for (uint32_t i = 0; i < ring->sq_entries; i++) {
        ring->sq_array[i] = i;
    }

    return ring;
}

void svdb_io_uring_destroy(svdb_io_uring_t* ring) {
    if (!ring || ring->ring_fd < 0) return;

    /* Wait for pending operations */
    uint32_t head = *ring->sq_head;
    uint32_t tail = ring->sq_tail_cached.load(std::memory_order_acquire);
    if (head != tail) {
        svdb_io_uring_submit_and_wait(ring, tail - head);
    }

    size_t sq_ring_size = ring->sq_entries * sizeof(uint32_t) + 32;
    size_t cq_ring_size = ring->cq_entries * sizeof(svdb_io_uring_cqe) + 32;
    size_t sqes_size = ring->sq_entries * sizeof(svdb_io_uring_sqe);

    munmap(ring->sqes, sqes_size);
    munmap(ring->cq_head, cq_ring_size);
    munmap(ring->sq_head, sq_ring_size);

    close(ring->ring_fd);
    delete ring;
}

int svdb_io_uring_submit(svdb_io_uring_t* ring) {
    if (!ring || ring->ring_fd < 0) return -1;

    uint32_t tail = ring->sq_tail_cached.load(std::memory_order_relaxed);
    uint32_t head = *ring->sq_head;
    uint32_t to_submit = tail - head;

    if (to_submit == 0) return 0;

    /* Ensure writes are visible before updating tail */
    std::atomic_thread_fence(std::memory_order_release);
    *ring->sq_tail = tail;

    int ret = sys_io_uring_enter(ring->ring_fd, to_submit, 0, 0, nullptr);
    return ret < 0 ? ret : static_cast<int>(to_submit);
}

int svdb_io_uring_submit_and_wait(svdb_io_uring_t* ring, uint32_t wait_nr) {
    if (!ring || ring->ring_fd < 0) return -1;

    uint32_t tail = ring->sq_tail_cached.load(std::memory_order_relaxed);
    uint32_t head = *ring->sq_head;
    uint32_t to_submit = tail - head;

    if (to_submit > 0) {
        std::atomic_thread_fence(std::memory_order_release);
        *ring->sq_tail = tail;
    }

    int ret = sys_io_uring_enter(ring->ring_fd, to_submit, wait_nr, 1, nullptr);
    return ret < 0 ? ret : static_cast<int>(wait_nr);
}

int svdb_io_uring_reap(svdb_io_uring_t* ring,
                       void (*callback)(uint64_t tag, int result, void* user_data),
                       void* user_data) {
    if (!ring || !callback) return 0;

    int count = 0;
    uint32_t head = *ring->cq_head;

    while (true) {
        uint32_t tail = *ring->cq_tail;
        if (head == tail) break;

        svdb_io_uring_cqe* cqe = &ring->cqes[head & ring->cq_ring_mask];
        callback(cqe->user_data, cqe->res, user_data);
        head++;
        count++;
    }

    std::atomic_thread_fence(std::memory_order_release);
    *ring->cq_head = head;

    return count;
}

int svdb_io_uring_available(void) {
    svdb_io_uring_params params = {};
    int fd = sys_io_uring_setup(1, &params);
    if (fd >= 0) {
        close(fd);
        return 1;
    }
    return 0;
}

/* ============================================================================
 * Async file operations
 * ============================================================================ */

svdb_io_file_t* svdb_io_uring_open_file(svdb_io_uring_t* ring,
                                          const char* path,
                                          int read_write,
                                          int create) {
    if (!ring || !path) return nullptr;

    int flags = O_CLOEXEC;
    if (read_write) {
        flags |= O_RDWR;
    } else {
        flags |= O_RDONLY;
    }
    if (create) {
        flags |= O_CREAT;
    }

    int fd = open(path, flags, 0644);
    if (fd < 0) return nullptr;

    svdb_io_file_t* file = new (std::nothrow) svdb_io_file_t();
    if (!file) {
        close(fd);
        return nullptr;
    }

    file->fd = fd;
    file->path = path;
    file->writable = read_write;

    struct stat st;
    if (fstat(fd, &st) == 0) {
        file->size = st.st_size;
    }

    return file;
}

void svdb_io_uring_close_file(svdb_io_file_t* file) {
    if (!file) return;
    if (file->fd >= 0) {
        close(file->fd);
    }
    delete file;
}

int svdb_io_uring_file_fd(svdb_io_file_t* file) {
    return file ? file->fd : -1;
}

int64_t svdb_io_uring_file_size(svdb_io_file_t* file) {
    return file ? file->size : -1;
}

/* ============================================================================
 * Async I/O operations
 * ============================================================================ */

static svdb_io_uring_sqe* get_sqe(svdb_io_uring_t* ring) {
    uint32_t tail = ring->sq_tail_cached.load(std::memory_order_relaxed);
    uint32_t head = *ring->sq_head;

    if (tail - head >= ring->sq_entries) {
        return nullptr; // Queue full
    }

    svdb_io_uring_sqe* sqe = &ring->sqes[tail & ring->sq_ring_mask];
    memset(sqe, 0, sizeof(*sqe));

    ring->sq_tail_cached.store(tail + 1, std::memory_order_relaxed);
    return sqe;
}

int svdb_io_uring_read(svdb_io_uring_t* ring,
                        svdb_io_file_t* file,
                        void* buffer,
                        size_t len,
                        int64_t offset,
                        uint64_t tag) {
    if (!ring || !file || file->fd < 0) return -1;

    svdb_io_uring_sqe* sqe = get_sqe(ring);
    if (!sqe) return -1;

    sqe->opcode = IORING_OP_READ;
    sqe->fd = file->fd;
    sqe->addr = (uint64_t)buffer;
    sqe->len = (uint32_t)len;
    sqe->off = (uint64_t)offset;
    sqe->user_data = tag;

    return 0;
}

int svdb_io_uring_write(svdb_io_uring_t* ring,
                         svdb_io_file_t* file,
                         const void* data,
                         size_t len,
                         int64_t offset,
                         uint64_t tag) {
    if (!ring || !file || file->fd < 0) return -1;

    svdb_io_uring_sqe* sqe = get_sqe(ring);
    if (!sqe) return -1;

    sqe->opcode = IORING_OP_WRITE;
    sqe->fd = file->fd;
    sqe->addr = (uint64_t)data;
    sqe->len = (uint32_t)len;
    sqe->off = (uint64_t)offset;
    sqe->user_data = tag;

    return 0;
}

int svdb_io_uring_readv(svdb_io_uring_t* ring,
                         svdb_io_file_t* file,
                         const struct iovec* iov,
                         size_t iov_count,
                         int64_t offset,
                         uint64_t tag) {
    if (!ring || !file || file->fd < 0) return -1;

    svdb_io_uring_sqe* sqe = get_sqe(ring);
    if (!sqe) return -1;

    sqe->opcode = IORING_OP_READV;
    sqe->fd = file->fd;
    sqe->addr = (uint64_t)iov;
    sqe->len = (uint32_t)iov_count;
    sqe->off = (uint64_t)offset;
    sqe->user_data = tag;

    return 0;
}

int svdb_io_uring_writev(svdb_io_uring_t* ring,
                          svdb_io_file_t* file,
                          const struct iovec* iov,
                          size_t iov_count,
                          int64_t offset,
                          uint64_t tag) {
    if (!ring || !file || file->fd < 0) return -1;

    svdb_io_uring_sqe* sqe = get_sqe(ring);
    if (!sqe) return -1;

    sqe->opcode = IORING_OP_WRITEV;
    sqe->fd = file->fd;
    sqe->addr = (uint64_t)iov;
    sqe->len = (uint32_t)iov_count;
    sqe->off = (uint64_t)offset;
    sqe->user_data = tag;

    return 0;
}

int svdb_io_uring_fsync(svdb_io_uring_t* ring,
                         svdb_io_file_t* file,
                         uint64_t tag) {
    if (!ring || !file || file->fd < 0) return -1;

    svdb_io_uring_sqe* sqe = get_sqe(ring);
    if (!sqe) return -1;

    sqe->opcode = IORING_OP_FSYNC;
    sqe->fd = file->fd;
    sqe->user_data = tag;

    return 0;
}

/* ============================================================================
 * Batch operations
 * ============================================================================ */

svdb_io_batch_t* svdb_io_batch_create(svdb_io_uring_t* ring) {
    if (!ring) return nullptr;
    return new (std::nothrow) svdb_io_batch_t(ring);
}

void svdb_io_batch_destroy(svdb_io_batch_t* batch) {
    delete batch;
}

int svdb_io_batch_add_write(svdb_io_batch_t* batch,
                             svdb_io_file_t* file,
                             const void* data,
                             size_t len,
                             int64_t offset,
                             uint64_t tag) {
    if (!batch || !file || !data) return -1;

    svdb_io_batch_t::BatchEntry entry;
    entry.file = file;
    entry.data.assign((const uint8_t*)data, (const uint8_t*)data + len);
    entry.offset = offset;
    entry.tag = tag;

    batch->entries.push_back(std::move(entry));
    return 0;
}

int svdb_io_batch_submit(svdb_io_batch_t* batch, int linked) {
    if (!batch || batch->entries.empty()) return 0;

    int count = 0;
    for (size_t i = 0; i < batch->entries.size(); i++) {
        auto& entry = batch->entries[i];
        svdb_io_uring_sqe* sqe = get_sqe(batch->ring);
        if (!sqe) break;

        sqe->opcode = IORING_OP_WRITE;
        sqe->fd = entry.file->fd;
        sqe->addr = (uint64_t)entry.data.data();
        sqe->len = (uint32_t)entry.data.size();
        sqe->off = (uint64_t)entry.offset;
        sqe->user_data = entry.tag;

        if (linked && i < batch->entries.size() - 1) {
            sqe->flags |= IOSQE_IO_LINK;
        }

        count++;
    }

    return count;
}

void svdb_io_batch_clear(svdb_io_batch_t* batch) {
    if (batch) batch->entries.clear();
}

size_t svdb_io_batch_count(svdb_io_batch_t* batch) {
    return batch ? batch->entries.size() : 0;
}

/* ============================================================================
 * Synchronous wrappers
 * ============================================================================ */

int64_t svdb_io_uring_read_sync(svdb_io_uring_t* ring,
                                 svdb_io_file_t* file,
                                 void* buffer,
                                 size_t len,
                                 int64_t offset) {
    if (!ring || !file || file->fd < 0) return -1;

    /* Simple fallback to pread for sync reads */
    return pread(file->fd, buffer, len, offset);
}

int64_t svdb_io_uring_write_sync(svdb_io_uring_t* ring,
                                  svdb_io_file_t* file,
                                  const void* data,
                                  size_t len,
                                  int64_t offset) {
    if (!ring || !file || file->fd < 0) return -1;

    /* Simple fallback to pwrite for sync writes */
    return pwrite(file->fd, data, len, offset);
}

#endif // __linux__

} // extern "C"