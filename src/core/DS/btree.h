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

// Page manager callbacks (provided by Go)
typedef struct {
    void* user_data;
    
    // Read a page from disk
    int (*read_page)(void* user_data, uint32_t page_num, uint8_t** page_data, size_t* page_size);
    
    // Write a page to disk
    int (*write_page)(void* user_data, uint32_t page_num, const uint8_t* page_data, size_t page_size);
    
    // Allocate a new page
    int (*allocate_page)(void* user_data, uint32_t* page_num);
    
    // Free a page
    int (*free_page)(void* user_data, uint32_t page_num);
} svdb_page_manager_t;

// Create a new B-Tree instance
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
#endif

#endif // SVDB_DS_BTREE_H
