/*
 * plan_cache.cpp — C++ plan cache for the CG subsystem.
 *
 * Uses std::unordered_map with a read-write mutex for O(1) average-case
 * SQL-to-program lookups.  Each entry stores the serialised optimised JSON
 * so the caller can reconstruct a VM.Program without re-running the
 * optimisation passes.
 */

#include "plan_cache.h"
#include <cstring>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

struct CacheEntry {
    std::string json;   /* serialised optimised program JSON */
};

struct PlanCacheImpl {
    std::mutex                              mu;
    std::unordered_map<std::string, CacheEntry> data;
};

extern "C" {

svdb_cg_cache_t* svdb_cg_cache_create(void)
{
    return reinterpret_cast<svdb_cg_cache_t*>(new PlanCacheImpl());
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
    std::lock_guard<std::mutex> lk(impl->mu);
    impl->data[sql] = CacheEntry{ std::string(json_data, json_len) };
}

const char* svdb_cg_cache_get_json(
    svdb_cg_cache_t* cache,
    const char*      sql,
    size_t*          out_len)
{
    auto* impl = reinterpret_cast<PlanCacheImpl*>(cache);
    std::lock_guard<std::mutex> lk(impl->mu);
    auto it = impl->data.find(sql);
    if (it == impl->data.end()) {
        *out_len = 0;
        return nullptr;
    }
    *out_len = it->second.json.size();
    return it->second.json.c_str();
}

/*
 * Thread-safe copy variant: copies the JSON string into a caller-provided
 * buffer under the mutex.  Returns true on hit, false on miss.
 * On hit with buf_size > json_len the buffer is NUL-terminated.
 */
bool svdb_cg_cache_copy_json(
    svdb_cg_cache_t* cache,
    const char*      sql,
    char*            buf,
    size_t           buf_size,
    size_t*          out_len)
{
    auto* impl = reinterpret_cast<PlanCacheImpl*>(cache);
    std::lock_guard<std::mutex> lk(impl->mu);
    auto it = impl->data.find(sql);
    if (it == impl->data.end()) {
        *out_len = 0;
        return false;
    }
    const std::string& j = it->second.json;
    *out_len = j.size();
    if (buf && buf_size > j.size()) {
        std::memcpy(buf, j.c_str(), j.size() + 1);
    }
    return true;
}

void svdb_cg_cache_erase(svdb_cg_cache_t* cache)
{
    auto* impl = reinterpret_cast<PlanCacheImpl*>(cache);
    std::lock_guard<std::mutex> lk(impl->mu);
    impl->data.clear();
}

size_t svdb_cg_cache_count(svdb_cg_cache_t* cache)
{
    auto* impl = reinterpret_cast<PlanCacheImpl*>(cache);
    std::lock_guard<std::mutex> lk(impl->mu);
    return impl->data.size();
}

} /* extern "C" */
