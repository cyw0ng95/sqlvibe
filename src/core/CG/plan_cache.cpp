/*
 * plan_cache.cpp — C++ plan cache for the CG subsystem.
 *
 * LRU cache implementation using std::list for access order tracking
 * and std::unordered_map for O(1) lookups. Uses shared_mutex for
 * read-write separation.
 */

#include "plan_cache.h"
#include <cstring>
#include <list>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>

struct LRUEntry {
    std::string sql;   /* cache key */
    std::string json;  /* serialised optimised program JSON */
};

struct PlanCacheImpl {
    mutable std::shared_mutex mu;
    std::unordered_map<std::string, std::list<LRUEntry>::iterator> index;
    std::list<LRUEntry> lru_list; /* Front = MRU, Back = LRU */
    size_t max_size;

    explicit PlanCacheImpl(size_t max_entries) : max_size(max_entries) {}
};

extern "C" {

svdb_cg_cache_t* svdb_cg_cache_create(void)
{
    return svdb_cg_cache_create_with_size(100);
}

svdb_cg_cache_t* svdb_cg_cache_create_with_size(size_t max_size)
{
    return reinterpret_cast<svdb_cg_cache_t*>(new PlanCacheImpl(max_size));
}

void svdb_cg_cache_free(svdb_cg_cache_t* cache)
{
    delete reinterpret_cast<PlanCacheImpl*>(cache);
}

void svdb_cg_cache_put_json(
    svdb_cg_cache_t* cache,
    const char*      sql,
    const char*      json_data,
    size_t           json_len)
{
    auto* impl = reinterpret_cast<PlanCacheImpl*>(cache);
    std::unique_lock<std::shared_mutex> lk(impl->mu);

    /* Check if key already exists */
    auto it = impl->index.find(sql);
    if (it != impl->index.end()) {
        /* Update existing entry and move to front */
        auto list_it = it->second;
        list_it->json.assign(json_data, json_len);
        impl->lru_list.splice(impl->lru_list.begin(), impl->lru_list, list_it);
        return;
    }

    /* Evict LRU entry if cache is full */
    if (impl->lru_list.size() >= impl->max_size) {
        auto& back = impl->lru_list.back();
        impl->index.erase(back.sql);
        impl->lru_list.pop_back();
    }

    /* Insert new entry at front of LRU list */
    impl->lru_list.push_front(LRUEntry{
        std::string(sql),
        std::string(json_data, json_len)
    });
    impl->index[sql] = impl->lru_list.begin();
}

const char* svdb_cg_cache_get_json(
    svdb_cg_cache_t* cache,
    const char*      sql,
    size_t*          out_len)
{
    auto* impl = reinterpret_cast<PlanCacheImpl*>(cache);
    std::unique_lock<std::shared_mutex> lk(impl->mu);

    auto it = impl->index.find(sql);
    if (it == impl->index.end()) {
        *out_len = 0;
        return nullptr;
    }

    /* Move to front (MRU position) */
    auto list_it = it->second;
    impl->lru_list.splice(impl->lru_list.begin(), impl->lru_list, list_it);

    *out_len = list_it->json.size();
    return list_it->json.c_str();
}

/*
 * Thread-safe copy variant: copies the JSON string into a caller-provided
 * buffer under a shared lock.  Returns true on hit, false on miss.
 * On hit with buf_size > json_len the buffer is NUL-terminated.
 *
 * Note: This uses a shared lock and does NOT update LRU order to allow
 * concurrent readers. For LRU updates on read, use svdb_cg_cache_get_json.
 */
bool svdb_cg_cache_copy_json(
    svdb_cg_cache_t* cache,
    const char*      sql,
    char*            buf,
    size_t           buf_size,
    size_t*          out_len)
{
    auto* impl = reinterpret_cast<PlanCacheImpl*>(cache);
    std::shared_lock<std::shared_mutex> lk(impl->mu);

    auto it = impl->index.find(sql);
    if (it == impl->index.end()) {
        *out_len = 0;
        return false;
    }

    const std::string& j = it->second->json;
    *out_len = j.size();
    if (buf && buf_size > j.size()) {
        std::memcpy(buf, j.c_str(), j.size() + 1);
    }
    return true;
}

void svdb_cg_cache_erase(svdb_cg_cache_t* cache)
{
    auto* impl = reinterpret_cast<PlanCacheImpl*>(cache);
    std::unique_lock<std::shared_mutex> lk(impl->mu);
    impl->index.clear();
    impl->lru_list.clear();
}

size_t svdb_cg_cache_count(svdb_cg_cache_t* cache)
{
    auto* impl = reinterpret_cast<PlanCacheImpl*>(cache);
    std::shared_lock<std::shared_mutex> lk(impl->mu);
    return impl->lru_list.size();
}

size_t svdb_cg_cache_max_size(svdb_cg_cache_t* cache)
{
    auto* impl = reinterpret_cast<PlanCacheImpl*>(cache);
    std::shared_lock<std::shared_mutex> lk(impl->mu);
    return impl->max_size;
}

} /* extern "C" */