/* worker_pool.cpp — Worker pool implementation with lock-free queue */
#include "worker_pool.h"
#include "lockfree_queue.h"
#include <thread>
#include <vector>
#include <atomic>
#include <condition_variable>

struct svdb_worker_pool_s {
    std::vector<std::thread> workers;
    svdb::ds::DefaultTaskQueue task_queue;  /* Lock-free MPMC queue */
    std::atomic<bool> stop;
    std::atomic<int> pending_tasks;
    std::condition_variable condition;  /* For blocking wait */
    std::mutex wait_mutex;  /* For condition variable */

    svdb_worker_pool_s() : stop(false), pending_tasks(0) {}
};

svdb_worker_pool_t* svdb_worker_pool_create(int workers) {
    if (workers < 1) workers = 1;

    svdb_worker_pool_t* pool = new svdb_worker_pool_t();

    for (int i = 0; i < workers; i++) {
        pool->workers.emplace_back([pool]() {
            while (true) {
                /* Try to pop from lock-free queue */
                auto result = pool->task_queue.try_pop();

                if (result.has_value()) {
                    /* Execute task */
                    if (result->fn) {
                        result->fn(result->user_data);
                        pool->pending_tasks--;
                    }
                    continue;  /* Try to get more tasks without blocking */
                }

                /* Queue is empty - check if we should stop */
                if (pool->stop) {
                    return;
                }

                /* Brief spin before yielding */
                for (int spin = 0; spin < 100; spin++) {
                    if (pool->stop) return;

                    result = pool->task_queue.try_pop();
                    if (result.has_value()) {
                        if (result->fn) {
                            result->fn(result->user_data);
                            pool->pending_tasks--;
                        }
                        goto next_task;  /* Continue outer loop */
                    }

                    /* CPU pause hint */
                    #if defined(__x86_64__) || defined(_M_X64)
                    __asm__ __volatile__("pause" ::: "memory");
                    #elif defined(__aarch64__)
                    __asm__ __volatile__("yield" ::: "memory");
                    #endif
                }

                /* Still empty - wait on condition variable */
                {
                    std::unique_lock<std::mutex> lock(pool->wait_mutex);
                    pool->condition.wait_for(lock, std::chrono::milliseconds(1), [pool]() {
                        return pool->stop || !pool->task_queue.empty();
                    });
                }

                next_task:;
            }
        });
    }

    return pool;
}

void svdb_worker_pool_destroy(svdb_worker_pool_t* pool) {
    if (!pool) return;

    svdb_worker_pool_close(pool);
    svdb_worker_pool_wait(pool);

    /* Join all worker threads */
    for (auto& worker : pool->workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    delete pool;
}

void svdb_worker_pool_submit(svdb_worker_pool_t* pool, svdb_task_fn task, void* user_data) {
    if (!pool || !task) return;

    svdb::ds::TaskItem item{task, user_data};
    pool->task_queue.push(std::move(item));
    pool->pending_tasks++;

    /* Notify one waiting worker */
    pool->condition.notify_one();
}

void svdb_worker_pool_wait(svdb_worker_pool_t* pool) {
    if (!pool) return;

    /* Spin-wait with yield for low latency */
    while (pool->pending_tasks > 0) {
        std::this_thread::yield();
    }
}

void svdb_worker_pool_close(svdb_worker_pool_t* pool) {
    if (!pool) return;

    pool->stop = true;
    pool->condition.notify_all();
}