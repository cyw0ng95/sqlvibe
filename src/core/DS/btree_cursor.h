#ifndef SVDB_DS_BTREE_CURSOR_H
#define SVDB_DS_BTREE_CURSOR_H

#include <cstdint>
#include <cstddef>
#include <vector>
#include <stack>

namespace svdb {
namespace ds {

struct BTreePage {
    uint32_t page_num;
    uint8_t* data;
    size_t data_size;
    bool is_leaf;
    bool is_dirty;
};

class BTreeCursor {
public:
    BTreeCursor();
    ~BTreeCursor();

    void SetTree(void* btree) { btree_ = btree; }
    void* GetTree() const { return btree_; }

    void Reset();
    
    bool First();
    bool Last();
    bool Next();
    bool Prev();
    bool Seek(const uint8_t* key, size_t key_len);
    
    bool IsValid() const { return valid_; }
    const uint8_t* GetKey() const { return current_key_.data(); }
    size_t GetKeyLen() const { return current_key_.size(); }
    const uint8_t* GetValue() const { return current_value_.data(); }
    size_t GetValueLen() const { return current_value_.size(); }

private:
    void* btree_;
    bool valid_;
    std::vector<uint8_t> current_key_;
    std::vector<uint8_t> current_value_;
    std::stack<uint32_t> page_stack_;
};

class PageCache {
public:
    PageCache(size_t max_pages);
    ~PageCache();

    BTreePage* GetPage(uint32_t page_num);
    void PutPage(uint32_t page_num, BTreePage* page);
    void Invalidate(uint32_t page_num);
    void Clear();

    size_t GetSize() const { return cache_size_; }
    size_t GetMaxSize() const { return max_pages_; }
    size_t GetHits() const { return hits_; }
    size_t GetMisses() const { return misses_; }

private:
    size_t max_pages_;
    size_t cache_size_;
    size_t hits_;
    size_t misses_;
};

} // namespace ds
} // namespace svdb

extern "C" {

void* SVDB_DS_BTreeCursor_Create();
void SVDB_DS_BTreeCursor_Destroy(void* cursor);

void SVDB_DS_BTreeCursor_Reset(void* cursor);
int SVDB_DS_BTreeCursor_First(void* cursor);
int SVDB_DS_BTreeCursor_Last(void* cursor);
int SVDB_DS_BTreeCursor_Next(void* cursor);
int SVDB_DS_BTreeCursor_Prev(void* cursor);
int SVDB_DS_BTreeCursor_Seek(void* cursor, const uint8_t* key, size_t key_len);

int SVDB_DS_BTreeCursor_IsValid(void* cursor);
const uint8_t* SVDB_DS_BTreeCursor_GetKey(void* cursor, size_t* out_len);
const uint8_t* SVDB_DS_BTreeCursor_GetValue(void* cursor, size_t* out_len);

void* SVDB_DS_PageCache_Create(size_t max_pages);
void SVDB_DS_PageCache_Destroy(void* cache);

void SVDB_DS_PageCache_Clear(void* cache);
size_t SVDB_DS_PageCache_GetSize(void* cache);
size_t SVDB_DS_PageCache_GetHits(void* cache);
size_t SVDB_DS_PageCache_GetMisses(void* cache);

} // extern "C"

#endif // SVDB_DS_BTREE_CURSOR_H
