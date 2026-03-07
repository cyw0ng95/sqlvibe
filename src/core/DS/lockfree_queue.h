/* lockfree_queue.h — Lock-free bounded MPMC queue
 * Based on Vyukov's bounded MPMC queue algorithm
 *
 * This implementation provides a wait-free bounded multi-producer multi-consumer queue
 * that can be used for task distribution and result collection in parallel execution.
 */
#ifndef SVDB_DS_LOCKFREE_QUEUE_H
#define SVDB_DS_LOCKFREE_QUEUE_H

#include <atomic>
#include <cstdint>
#include <cstddef>
#include <memory>
#include <type_traits>
#include <optional>

namespace svdb::ds {

/*
 * VyukovMPMCQueue — A bounded MPMC (Multi-Producer Multi-Consumer) queue
 *
 * This is a lock-free queue that supports multiple producers and consumers
 * concurrently without mutexes. It uses a bounded ring buffer with atomic
 * sequences for coordination.
 *
 * Template parameters:
 *   T - the element type (must be trivially copyable or movable)
 *   Capacity - the queue capacity (must be a power of 2)
 */
template<typename T, size_t Capacity>
class VyukovMPMCQueue {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of 2");
    static_assert(Capacity > 0, "Capacity must be greater than 0");
    static_assert(std::is_trivially_copyable_v<T> || std::is_move_constructible_v<T>,
                  "T must be trivially copyable or move constructible");

public:
    VyukovMPMCQueue() : head_(0), tail_(0) {
        // Initialize sequences
        for (size_t i = 0; i < Capacity; ++i) {
            buffer_[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    ~VyukovMPMCQueue() = default;

    // Non-copyable, non-movable
    VyukovMPMCQueue(const VyukovMPMCQueue&) = delete;
    VyukovMPMCQueue& operator=(const VyukovMPMCQueue&) = delete;
    VyukovMPMCQueue(VyukovMPMCQueue&&) = delete;
    VyukovMPMCQueue& operator=(VyukovMPMCQueue&&) = delete;

    /*
     * try_push — Try to push an element to the queue (non-blocking)
     * Returns true on success, false if queue is full
     */
    bool try_push(T&& item) {
        Cell* cell;
        size_t pos = tail_.load(std::memory_order_relaxed);

        for (;;) {
            cell = &buffer_[pos & Mask];
            size_t seq = cell->sequence.load(std::memory_order_acquire);
            intptr_t diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);

            if (diff == 0) {
                if (tail_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    break;
                }
            } else if (diff < 0) {
                return false; // Queue is full
            } else {
                pos = tail_.load(std::memory_order_relaxed);
            }
        }

        cell->data = std::move(item);
        cell->sequence.store(pos + 1, std::memory_order_release);
        return true;
    }

    /*
     * try_push_copy — Try to push by copying (for trivially copyable types)
     */
    bool try_push_copy(const T& item) {
        T copy = item;
        return try_push(std::move(copy));
    }

    /*
     * try_pop — Try to pop an element from the queue (non-blocking)
     * Returns the element on success, nullopt if queue is empty
     */
    std::optional<T> try_pop() {
        Cell* cell;
        size_t pos = head_.load(std::memory_order_relaxed);

        for (;;) {
            cell = &buffer_[pos & Mask];
            size_t seq = cell->sequence.load(std::memory_order_acquire);
            intptr_t diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);

            if (diff == 0) {
                if (head_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    break;
                }
            } else if (diff < 0) {
                return std::nullopt; // Queue is empty
            } else {
                pos = head_.load(std::memory_order_relaxed);
            }
        }

        T result = std::move(cell->data);
        cell->sequence.store(pos + Mask + 1, std::memory_order_release);
        return result;
    }

    /*
     * push — Push with spin-wait (blocking until space is available)
     */
    void push(T&& item) {
        while (!try_push(std::move(item))) {
            // Spin with pause instruction hint
            #if defined(__x86_64__) || defined(_M_X64)
            __asm__ __volatile__("pause" ::: "memory");
            #elif defined(__aarch64__)
            __asm__ __volatile__("yield" ::: "memory");
            #endif
        }
    }

    /*
     * pop — Pop with spin-wait (blocking until element is available)
     */
    T pop() {
        while (true) {
            auto result = try_pop();
            if (result.has_value()) {
                return std::move(result.value());
            }
            #if defined(__x86_64__) || defined(_M_X64)
            __asm__ __volatile__("pause" ::: "memory");
            #elif defined(__aarch64__)
            __asm__ __volatile__("yield" ::: "memory");
            #endif
        }
    }

    /*
     * empty — Check if queue is empty (approximate, for optimization purposes)
     * Note: This is not atomic and may give stale results
     */
    bool empty() const {
        size_t tail = tail_.load(std::memory_order_relaxed);
        size_t head = head_.load(std::memory_order_relaxed);
        return tail == head;
    }

    /*
     * size — Get approximate size (may be stale)
     */
    size_t size() const {
        size_t tail = tail_.load(std::memory_order_relaxed);
        size_t head = head_.load(std::memory_order_relaxed);
        return (tail >= head) ? (tail - head) : 0;
    }

    static constexpr size_t capacity() { return Capacity; }

private:
    static constexpr size_t Mask = Capacity - 1;

    struct Cell {
        std::atomic<size_t> sequence;
        T data;
    };

    alignas(64) std::atomic<size_t> head_;
    alignas(64) std::atomic<size_t> tail_;
    alignas(64) Cell buffer_[Capacity];
};

/*
 * LockFreeTaskQueue — Specialized task queue for worker pool
 * Uses 1024-element bounded queue by default
 */
struct TaskItem {
    void (*fn)(void*) = nullptr;
    void* user_data = nullptr;
};

using DefaultTaskQueue = VyukovMPMCQueue<TaskItem, 1024>;

} // namespace svdb::ds

/* C API for Go bindings */
extern "C" {

/* Opaque handle for lock-free task queue */
struct svdb_lockfree_task_queue_t;

/* Create a lock-free task queue with default capacity (1024) */
svdb_lockfree_task_queue_t* svdb_lockfree_task_queue_create(void);

/* Destroy the queue */
void svdb_lockfree_task_queue_destroy(svdb_lockfree_task_queue_t* queue);

/* Push a task (non-blocking, returns 1 on success, 0 if full) */
int svdb_lockfree_task_queue_push(svdb_lockfree_task_queue_t* queue,
                                   void (*fn)(void*), void* user_data);

/* Pop a task (non-blocking, returns 1 on success, 0 if empty) */
int svdb_lockfree_task_queue_pop(svdb_lockfree_task_queue_t* queue,
                                  void (**out_fn)(void*), void** out_user_data);

/* Push with spin-wait (blocking until space available) */
void svdb_lockfree_task_queue_push_wait(svdb_lockfree_task_queue_t* queue,
                                         void (*fn)(void*), void* user_data);

/* Pop with spin-wait (blocking until task available) */
void svdb_lockfree_task_queue_pop_wait(svdb_lockfree_task_queue_t* queue,
                                        void (**out_fn)(void*), void** out_user_data);

/* Check if queue is empty (approximate) */
int svdb_lockfree_task_queue_empty(svdb_lockfree_task_queue_t* queue);

/* Get approximate size */
size_t svdb_lockfree_task_queue_size(svdb_lockfree_task_queue_t* queue);

} // extern "C"

#endif // SVDB_DS_LOCKFREE_QUEUE_H