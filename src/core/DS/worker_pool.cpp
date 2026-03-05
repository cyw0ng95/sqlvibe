/* worker_pool.cpp — Worker pool implementation */
#include "worker_pool.h"
#include <thread>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <functional>

struct svdb_worker_pool_s {
    std::vector<std::thread> workers;
    std::queue<std::pair<svdb_task_fn, void*>> tasks;
    std::mutex queue_mutex;
    std::condition_variable condition;
    std::atomic<bool> stop;
    std::atomic<int> pending_tasks;
};

svdb_worker_pool_t* svdb_worker_pool_create(int workers) {
    if (workers < 1) workers = 1;
    
    svdb_worker_pool_t* pool = new svdb_worker_pool_t();
    pool->stop = false;
    pool->pending_tasks = 0;
    
    for (int i = 0; i < workers; i++) {
        pool->workers.emplace_back([pool]() {
            while (true) {
                std::pair<svdb_task_fn, void*> task;
                {
                    std::unique_lock<std::mutex> lock(pool->queue_mutex);
                    pool->condition.wait(lock, [pool]() {
                        return pool->stop || !pool->tasks.empty();
                    });
                    
                    if (pool->stop && pool->tasks.empty()) {
                        return;
                    }
                    
                    task = pool->tasks.front();
                    pool->tasks.pop();
                }
                
                /* Execute task */
                if (task.first) {
                    task.first(task.second);
                    pool->pending_tasks--;
                }
            }
        });
    }
    
    return pool;
}

void svdb_worker_pool_destroy(svdb_worker_pool_t* pool) {
    if (!pool) return;
    
    svdb_worker_pool_close(pool);
    svdb_worker_pool_wait(pool);
    
    delete pool;
}

void svdb_worker_pool_submit(svdb_worker_pool_t* pool, svdb_task_fn task, void* user_data) {
    if (!pool || !task) return;
    
    {
        std::unique_lock<std::mutex> lock(pool->queue_mutex);
        pool->tasks.push({task, user_data});
        pool->pending_tasks++;
    }
    pool->condition.notify_one();
}

void svdb_worker_pool_wait(svdb_worker_pool_t* pool) {
    if (!pool) return;
    
    while (pool->pending_tasks > 0) {
        std::this_thread::yield();
    }
}

void svdb_worker_pool_close(svdb_worker_pool_t* pool) {
    if (!pool) return;
    
    {
        std::unique_lock<std::mutex> lock(pool->queue_mutex);
        pool->stop = true;
    }
    pool->condition.notify_all();
}
