#ifndef SVDB_DS_BTREE_H
#define SVDB_DS_BTREE_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// B-Tree handle (opaque)
typedef struct svdb_btree svdb_btree_t;

// B-Tree configuration
typedef struct {
    uint32_t root_page;
    int is_table;  // 1 for table B-Tree, 0 for index B-Tree
    uint32_t page_size;
} svdb_btree_config_t;

// Page manager callback function types
typedef int (*read_page_fn)(void* user_data, uint32_t page_num, uint8_t** page_data, size_t* page_size);
typedef int (*write_page_fn)(void* user_data, uint32_t page_num, const uint8_t* page_data, size_t page_size);
typedef int (*allocate_page_fn)(void* user_data, uint32_t* page_num);
typedef int (*free_page_fn)(void* user_data, uint32_t page_num);

// Page manager callbacks (provided by Go)
typedef struct {
    void* user_data;

    // Read a page from disk
    read_page_fn read_page;

    // Write a page to disk
    write_page_fn write_page;

    // Allocate a new page
    allocate_page_fn allocate_page;

    // Free a page
    free_page_fn free_page;
} svdb_page_manager_t;

// Create a new B-Tree instance with embedded C++ PageManager (no Go callbacks)
// This is the preferred method for v0.11.0+ - eliminates Go callback overhead
// Parameters:
//   - db_path: Path to the SQLite database file
//   - root_page: Root page number (1-based, 0 for new B-Tree)
//   - is_table: 1 for table B-Tree, 0 for index B-Tree
//   - page_size: Page size (must be valid per svdb_manager_is_valid_page_size)
//   - cache_pages: Number of pages to cache (0 = use default 2000)
//
// Returns: B-Tree handle, or NULL on error
svdb_btree_t* svdb_btree_create_embedded(const char* db_path, uint32_t root_page,
                                         int is_table, uint32_t page_size, int cache_pages);

// Create a new B-Tree instance (with Go callbacks - legacy, deprecated)
// Only use this if you cannot use the embedded version.
// Parameters:
//   - config: B-Tree configuration
//   - pm: Page manager callbacks (provided by Go)
//
// Returns: B-Tree handle, or NULL on error
svdb_btree_t* svdb_btree_create(const svdb_btree_config_t* config, const svdb_page_manager_t* pm);

// Destroy a B-Tree instance
void svdb_btree_destroy(svdb_btree_t* bt);

// Search for a key in the B-Tree
// Returns 1 if found (value populated), 0 if not found
int svdb_btree_search(svdb_btree_t* bt, const uint8_t* key, size_t key_len, 
                      uint8_t** value, size_t* value_len);

// Insert a key-value pair into the B-Tree
// Returns 1 on success, 0 on error
int svdb_btree_insert(svdb_btree_t* bt, const uint8_t* key, size_t key_len,
                      const uint8_t* value, size_t value_len);

// Delete a key from the B-Tree
// Returns 1 on success, 0 if not found or error
int svdb_btree_delete(svdb_btree_t* bt, const uint8_t* key, size_t key_len);

// Get statistics
uint32_t svdb_btree_get_depth(svdb_btree_t* bt);
uint32_t svdb_btree_get_leaf_count(svdb_btree_t* bt);

// Binary search in a page (low-level function)
// Returns cell index or -1 if not found
int svdb_btree_binary_search(const uint8_t* page_data, size_t page_size,
                              const uint8_t* key, size_t key_len, int is_table);

#ifdef __cplusplus
}

#include <string>

namespace svdb {
namespace ds {

// C++ wrapper for embedded B-Tree
class BTreeEmbedded {
public:
    BTreeEmbedded(const std::string& db_path, uint32_t root_page, 
                  bool is_table, uint32_t page_size, int cache_pages = 0);
    ~BTreeEmbedded();

    // Delete copy
    BTreeEmbedded(const BTreeEmbedded&) = delete;
    BTreeEmbedded& operator=(const BTreeEmbedded&) = delete;

    // Search - returns true if found
    bool Search(const uint8_t* key, size_t key_len, uint8_t** value, size_t* value_len);

    // Insert
    bool Insert(const uint8_t* key, size_t key_len, const uint8_t* value, size_t value_len);

    // Delete
    bool Delete(const uint8_t* key, size_t key_len);

    // Stats
    uint32_t GetDepth() const;
    uint32_t GetLeafCount() const;

    // Sync changes to disk
    bool Sync();

    // Check if valid
    bool IsValid() const { return btree_ != nullptr; }

private:
    svdb_btree_t* btree_;
};

} // namespace ds
} // namespace svdb

extern "C" {
#endif

#ifdef __cplusplus
}
#endif

#endif // SVDB_DS_BTREE_H
