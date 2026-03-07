/* lockfree_queue.cpp — Lock-free bounded MPMC queue C API implementation */
#include "lockfree_queue.h"
#include <cstdlib>

extern "C" {

struct svdb_lockfree_task_queue_t {
    svdb::ds::DefaultTaskQueue queue;
};

svdb_lockfree_task_queue_t* svdb_lockfree_task_queue_create(void) {
    try {
        return new svdb_lockfree_task_queue_t();
    } catch (...) {
        return nullptr;
    }
}

void svdb_lockfree_task_queue_destroy(svdb_lockfree_task_queue_t* queue) {
    delete queue;
}

int svdb_lockfree_task_queue_push(svdb_lockfree_task_queue_t* queue,
                                   void (*fn)(void*), void* user_data) {
    if (!queue || !fn) return 0;

    svdb::ds::TaskItem item{fn, user_data};
    return queue->queue.try_push(std::move(item)) ? 1 : 0;
}

int svdb_lockfree_task_queue_pop(svdb_lockfree_task_queue_t* queue,
                                  void (**out_fn)(void*), void** out_user_data) {
    if (!queue || !out_fn || !out_user_data) return 0;

    auto result = queue->queue.try_pop();
    if (result.has_value()) {
        *out_fn = result->fn;
        *out_user_data = result->user_data;
        return 1;
    }
    return 0;
}

void svdb_lockfree_task_queue_push_wait(svdb_lockfree_task_queue_t* queue,
                                         void (*fn)(void*), void* user_data) {
    if (!queue || !fn) return;

    svdb::ds::TaskItem item{fn, user_data};
    queue->queue.push(std::move(item));
}

void svdb_lockfree_task_queue_pop_wait(svdb_lockfree_task_queue_t* queue,
                                        void (**out_fn)(void*), void** out_user_data) {
    if (!queue || !out_fn || !out_user_data) return;

    auto result = queue->queue.pop();
    *out_fn = result.fn;
    *out_user_data = result.user_data;
}

int svdb_lockfree_task_queue_empty(svdb_lockfree_task_queue_t* queue) {
    if (!queue) return 1;
    return queue->queue.empty() ? 1 : 0;
}

size_t svdb_lockfree_task_queue_size(svdb_lockfree_task_queue_t* queue) {
    if (!queue) return 0;
    return queue->queue.size();
}

} // extern "C"