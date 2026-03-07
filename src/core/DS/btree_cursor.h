#ifndef SVDB_DS_BTREE_CURSOR_H
#define SVDB_DS_BTREE_CURSOR_H

#include <cstdint>
#include <cstddef>
#include <vector>
#include <stack>

namespace svdb {
namespace ds {

// Page type constants (SQLite-compatible)
constexpr uint8_t PAGE_TYPE_TABLE_LEAF = 0x0d;
constexpr uint8_t PAGE_TYPE_TABLE_INTERIOR = 0x05;
constexpr uint8_t PAGE_TYPE_INDEX_LEAF = 0x0a;
constexpr uint8_t PAGE_TYPE_INDEX_INTERIOR = 0x02;

// Forward declaration
struct svdb_page_manager;

// Cursor stack entry for tracking navigation path
struct CursorStackEntry {
    uint32_t page_num;    // Page number
    int cell_index;       // Cell index within the page
};

struct BTreePage {
    uint32_t page_num;
    uint8_t* data;
    size_t data_size;
    bool is_leaf;
    bool is_dirty;
};

// Page access interface - implemented by BTree owner
class PageAccessor {
public:
    virtual ~PageAccessor() = default;
    virtual int ReadPage(uint32_t page_num, const uint8_t** data, size_t* size) = 0;
    virtual uint32_t GetRootPage() const = 0;
    virtual bool IsTableBTree() const = 0;
};

class BTreeCursor {
public:
    BTreeCursor();
    explicit BTreeCursor(PageAccessor* accessor);
    ~BTreeCursor();

    void SetAccessor(PageAccessor* accessor) { accessor_ = accessor; }
    PageAccessor* GetAccessor() const { return accessor_; }

    // Legacy API - kept for compatibility
    void SetTree(void* btree) { btree_ = btree; }
    void* GetTree() const { return btree_; }

    void Reset();

    // Navigation methods
    bool First();   // Move to first (leftmost) entry
    bool Last();    // Move to last (rightmost) entry
    bool Next();    // Move to next entry in key order
    bool Prev();    // Move to previous entry in key order
    bool Seek(const uint8_t* key, size_t key_len);

    bool IsValid() const { return valid_; }
    bool IsEof() const { return eof_; }

    // Current position
    uint32_t GetPageNum() const { return current_page_; }
    int GetCellIndex() const { return cell_index_; }

    // Data access
    const uint8_t* GetKey() const { return current_key_.data(); }
    size_t GetKeyLen() const { return current_key_.size(); }
    const uint8_t* GetValue() const { return current_value_.data(); }
    size_t GetValueLen() const { return current_value_.size(); }

    // For table B-Trees, get the current rowid
    int64_t GetRowid() const { return current_rowid_; }

private:
    // Internal navigation helpers (SQLite-style)
    bool MoveToLeftmost();   // Descend to leftmost leaf cell
    bool MoveToRightmost();  // Descend to rightmost leaf cell

    // Page operations
    bool ReadCurrentPage();
    int GetNumCells() const;
    uint16_t GetCellOffset(int cell_idx) const;
    bool IsLeafPage() const;
    bool IsTablePage() const;
    uint8_t GetPageType() const;

    // Cell parsing
    bool DecodeCurrentCell();
    bool DecodeTableLeafCell(uint16_t cell_offset);
    bool DecodeTableInteriorCell(uint16_t cell_offset, uint32_t* left_child, int64_t* rowid);
    bool DecodeIndexLeafCell(uint16_t cell_offset);
    bool DecodeIndexInteriorCell(uint16_t cell_offset, uint32_t* left_child);

    // Navigation helpers
    bool DescendToLeftmost(uint32_t page_num);
    bool DescendToRightmost(uint32_t page_num);
    bool AscendAndMoveNext();
    bool AscendAndMovePrev();

    // Members
    void* btree_;                      // Legacy - opaque BTree pointer
    PageAccessor* accessor_;           // Page accessor interface

    // Cursor state
    bool valid_;                       // Cursor is positioned on valid entry
    bool eof_;                         // Cursor has reached end

    // Current position
    uint32_t current_page_;            // Current page number
    int cell_index_;                   // Current cell index within page
    const uint8_t* page_data_;         // Cached page data pointer
    size_t page_size_;                 // Current page size

    // Navigation stack (for traversing interior nodes)
    std::vector<CursorStackEntry> nav_stack_;

    // Current key/value data
    std::vector<uint8_t> current_key_;
    std::vector<uint8_t> current_value_;
    int64_t current_rowid_;            // For table B-Trees
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

// BTreeCursor C API
void* SVDB_DS_BTreeCursor_Create();
void SVDB_DS_BTreeCursor_Destroy(void* cursor);

void SVDB_DS_BTreeCursor_Reset(void* cursor);
int SVDB_DS_BTreeCursor_First(void* cursor);
int SVDB_DS_BTreeCursor_Last(void* cursor);
int SVDB_DS_BTreeCursor_Next(void* cursor);
int SVDB_DS_BTreeCursor_Prev(void* cursor);
int SVDB_DS_BTreeCursor_Seek(void* cursor, const uint8_t* key, size_t key_len);

int SVDB_DS_BTreeCursor_IsValid(void* cursor);
int SVDB_DS_BTreeCursor_IsEof(void* cursor);
const uint8_t* SVDB_DS_BTreeCursor_GetKey(void* cursor, size_t* out_len);
const uint8_t* SVDB_DS_BTreeCursor_GetValue(void* cursor, size_t* out_len);
int64_t SVDB_DS_BTreeCursor_GetRowid(void* cursor);

// PageCache C API
void* SVDB_DS_PageCache_Create(size_t max_pages);
void SVDB_DS_PageCache_Destroy(void* cache);

void SVDB_DS_PageCache_Clear(void* cache);
size_t SVDB_DS_PageCache_GetSize(void* cache);
size_t SVDB_DS_PageCache_GetHits(void* cache);
size_t SVDB_DS_PageCache_GetMisses(void* cache);

} // extern "C"

#endif // SVDB_DS_BTREE_CURSOR_H
