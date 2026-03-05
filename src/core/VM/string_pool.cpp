#include "string_pool.h"
#include <cstring>

namespace svdb {

StringPool& StringPool::instance() {
    static StringPool pool;
    return pool;
}

const char* StringPool::intern(const char* str, size_t len) {
    if (!str || len == 0) {
        return nullptr;
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Check if already exists
    std::string key(str, len);
    auto it = strings_.find(key);
    if (it != strings_.end()) {
        return it->c_str();
    }
    
    // Insert and return pointer to stored string
    auto result = strings_.insert(std::move(key));
    return result.first->c_str();
}

const char* StringPool::intern(const std::string& s) {
    return intern(s.c_str(), s.size());
}

bool StringPool::is_interned(const char* str, size_t len) const {
    if (!str || len == 0) {
        return false;
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    std::string key(str, len);
    return strings_.find(key) != strings_.end();
}

size_t StringPool::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return strings_.size();
}

void StringPool::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    strings_.clear();
}

} // namespace svdb

// C-compatible API
extern "C" {

const char* svdb_string_intern(const char* str, size_t len) {
    return svdb::StringPool::instance().intern(str, len);
}

int svdb_string_is_interned(const char* str, size_t len) {
    return svdb::StringPool::instance().is_interned(str, len) ? 1 : 0;
}

size_t svdb_string_pool_size(void) {
    return svdb::StringPool::instance().size();
}

void svdb_string_pool_clear(void) {
    svdb::StringPool::instance().clear();
}

} // extern "C"
