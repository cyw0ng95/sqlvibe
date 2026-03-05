#ifndef SVDB_VM_STRING_POOL_H
#define SVDB_VM_STRING_POOL_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
#include <string>
#include <unordered_set>
#include <mutex>

namespace svdb {

// StringPool provides string interning for memory-efficient string handling.
// Identical strings share a single canonical copy, reducing memory usage
// and enabling pointer-based comparisons in hot paths.
class StringPool {
public:
    static StringPool& instance();
    
    // Intern a string and return a pointer to the canonical copy
    const char* intern(const char* str, size_t len);
    
    // Intern a std::string
    const char* intern(const std::string& s);
    
    // Check if a string is already interned
    bool is_interned(const char* str, size_t len) const;
    
    // Get the number of interned strings
    size_t size() const;
    
    // Clear the pool (for testing/debugging)
    void clear();

private:
    StringPool() = default;
    ~StringPool() = default;
    StringPool(const StringPool&) = delete;
    StringPool& operator=(const StringPool&) = delete;
    
    mutable std::mutex mutex_;
    std::unordered_set<std::string> strings_;
};

} // namespace svdb
#endif // __cplusplus

#ifdef __cplusplus
extern "C" {
#endif

// C-compatible API for string interning
// Returns a pointer to the interned string (caller must not free)
const char* svdb_string_intern(const char* str, size_t len);

// Check if a string is already interned
int svdb_string_is_interned(const char* str, size_t len);

// Get the number of interned strings
size_t svdb_string_pool_size(void);

// Clear the string pool
void svdb_string_pool_clear(void);

#ifdef __cplusplus
}
#endif

#endif // SVDB_VM_STRING_POOL_H
