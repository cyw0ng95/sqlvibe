#include "btree_cursor.h"
#include "varint.h"
#include "cell.h"
#include "../SF/svdb_assert.h"
#include <algorithm>
#include <cstring>

namespace svdb {
namespace ds {

BTreeCursor::BTreeCursor()
    : btree_(nullptr)
    , accessor_(nullptr)
    , valid_(false)
    , eof_(false)
    , current_page_(0)
    , cell_index_(-1)
    , page_data_(nullptr)
    , page_size_(0)
    , current_rowid_(0) {
}

BTreeCursor::BTreeCursor(PageAccessor* accessor)
    : btree_(nullptr)
    , accessor_(accessor)
    , valid_(false)
    , eof_(false)
    , current_page_(0)
    , cell_index_(-1)
    , page_data_(nullptr)
    , page_size_(0)
    , current_rowid_(0) {
}

BTreeCursor::~BTreeCursor() {
    // No owned resources to clean up
}

void BTreeCursor::Reset() {
    valid_ = false;
    eof_ = false;
    current_page_ = 0;
    cell_index_ = -1;
    page_data_ = nullptr;
    page_size_ = 0;
    current_key_.clear();
    current_value_.clear();
    current_rowid_ = 0;
    nav_stack_.clear();
}

// ============================================================================
// Page Operations
// ============================================================================

bool BTreeCursor::ReadCurrentPage() {
    if (!accessor_ || current_page_ == 0) {
        return false;
    }
    return accessor_->ReadPage(current_page_, &page_data_, &page_size_) == 1;
}

uint8_t BTreeCursor::GetPageType() const {
    if (!page_data_ || page_size_ < 1) {
        return 0;
    }
    return page_data_[0];
}

bool BTreeCursor::IsLeafPage() const {
    uint8_t type = GetPageType();
    return type == PAGE_TYPE_TABLE_LEAF || type == PAGE_TYPE_INDEX_LEAF;
}

bool BTreeCursor::IsTablePage() const {
    uint8_t type = GetPageType();
    return type == PAGE_TYPE_TABLE_LEAF || type == PAGE_TYPE_TABLE_INTERIOR;
}

int BTreeCursor::GetNumCells() const {
    if (!page_data_ || page_size_ < 8) {
        return 0;
    }
    // Cell count is at bytes 3-4 (big-endian)
    return (page_data_[3] << 8) | page_data_[4];
}

uint16_t BTreeCursor::GetCellOffset(int cell_idx) const {
    if (!page_data_ || page_size_ < 12 || cell_idx < 0) {
        return 0;
    }
    // Cell pointer array starts at byte 8
    int ptr_offset = 8 + cell_idx * 2;
    if (ptr_offset + 2 > static_cast<int>(page_size_)) {
        return 0;
    }
    return (page_data_[ptr_offset] << 8) | page_data_[ptr_offset + 1];
}

// ============================================================================
// Cell Parsing
// ============================================================================

bool BTreeCursor::DecodeTableLeafCell(uint16_t cell_offset) {
    if (!page_data_ || cell_offset >= page_size_) {
        return false;
    }

    svdb_cell_data_t cell;
    if (!svdb_decode_table_leaf_cell(page_data_ + cell_offset, page_size_ - cell_offset, &cell)) {
        return false;
    }

    current_rowid_ = cell.rowid;

    // For table B-Trees, the key is the rowid encoded as varint
    current_key_.resize(9);
    int key_len = svdb_put_varint(current_key_.data(), 9, cell.rowid);
    current_key_.resize(key_len);

    // Copy payload
    current_value_.assign(cell.payload, cell.payload + cell.payload_len);

    svdb_free_cell_data(&cell);
    return true;
}

bool BTreeCursor::DecodeTableInteriorCell(uint16_t cell_offset, uint32_t* left_child, int64_t* rowid) {
    if (!page_data_ || cell_offset >= page_size_ || !left_child || !rowid) {
        return false;
    }

    svdb_cell_data_t cell;
    if (!svdb_decode_table_interior_cell(page_data_ + cell_offset, page_size_ - cell_offset, &cell)) {
        return false;
    }

    *left_child = cell.left_child;
    *rowid = cell.rowid;

    svdb_free_cell_data(&cell);
    return true;
}

bool BTreeCursor::DecodeIndexLeafCell(uint16_t cell_offset) {
    if (!page_data_ || cell_offset >= page_size_) {
        return false;
    }

    svdb_cell_data_t cell;
    if (!svdb_decode_index_leaf_cell(page_data_ + cell_offset, page_size_ - cell_offset, &cell)) {
        return false;
    }

    // Copy key
    current_key_.assign(cell.key, cell.key + cell.key_len);

    // Copy payload (which contains the rest of the index entry)
    current_value_.assign(cell.payload, cell.payload + cell.payload_len);
    current_rowid_ = 0;

    svdb_free_cell_data(&cell);
    return true;
}

bool BTreeCursor::DecodeIndexInteriorCell(uint16_t cell_offset, uint32_t* left_child) {
    if (!page_data_ || cell_offset >= page_size_ || !left_child) {
        return false;
    }

    svdb_cell_data_t cell;
    if (!svdb_decode_index_interior_cell(page_data_ + cell_offset, page_size_ - cell_offset, &cell)) {
        return false;
    }

    *left_child = cell.left_child;

    svdb_free_cell_data(&cell);
    return true;
}

bool BTreeCursor::DecodeCurrentCell() {
    if (cell_index_ < 0 || cell_index_ >= GetNumCells()) {
        return false;
    }

    uint16_t cell_offset = GetCellOffset(cell_index_);
    if (cell_offset == 0 || cell_offset >= page_size_) {
        return false;
    }

    uint8_t page_type = GetPageType();

    switch (page_type) {
        case PAGE_TYPE_TABLE_LEAF:
            return DecodeTableLeafCell(cell_offset);

        case PAGE_TYPE_INDEX_LEAF:
            return DecodeIndexLeafCell(cell_offset);

        // Interior pages don't have data to decode for leaf traversal
        case PAGE_TYPE_TABLE_INTERIOR:
        case PAGE_TYPE_INDEX_INTERIOR:
            return false;

        default:
            svdb_assert_msg(false, "invalid page type: 0x%02x", page_type);
            return false;
    }
}

// ============================================================================
// Navigation - MoveToLeftmost/MoveToRightmost (SQLite-style)
// ============================================================================

bool BTreeCursor::DescendToLeftmost(uint32_t page_num) {
    current_page_ = page_num;
    cell_index_ = 0;

    while (true) {
        if (!ReadCurrentPage()) {
            return false;
        }

        if (IsLeafPage()) {
            // Found leftmost leaf
            int num_cells = GetNumCells();
            if (num_cells == 0) {
                // Empty leaf
                eof_ = true;
                valid_ = false;
                return false;
            }
            cell_index_ = 0;
            return DecodeCurrentCell();
        }

        // Interior page: push current position and descend to leftmost child
        CursorStackEntry entry;
        entry.page_num = current_page_;
        entry.cell_index = 0;  // We're taking the leftmost path
        nav_stack_.push_back(entry);

        // Get leftmost child (first cell's left child or rightmost pointer)
        uint16_t cell_offset = GetCellOffset(0);
        uint32_t left_child = 0;

        if (IsTablePage()) {
            int64_t rowid;
            if (!DecodeTableInteriorCell(cell_offset, &left_child, &rowid)) {
                return false;
            }
        } else {
            if (!DecodeIndexInteriorCell(cell_offset, &left_child)) {
                return false;
            }
        }

        if (left_child == 0) {
            return false;
        }

        current_page_ = left_child;
        cell_index_ = 0;
    }
}

bool BTreeCursor::DescendToRightmost(uint32_t page_num) {
    current_page_ = page_num;
    cell_index_ = -1;

    while (true) {
        if (!ReadCurrentPage()) {
            return false;
        }

        int num_cells = GetNumCells();

        if (IsLeafPage()) {
            // Found rightmost leaf
            if (num_cells == 0) {
                // Empty leaf
                eof_ = true;
                valid_ = false;
                return false;
            }
            cell_index_ = num_cells - 1;
            return DecodeCurrentCell();
        }

        // Interior page: push current position and descend to rightmost child
        CursorStackEntry entry;
        entry.page_num = current_page_;
        entry.cell_index = num_cells;  // Position past last cell (rightmost child)
        nav_stack_.push_back(entry);

        // Get rightmost child (stored in header bytes 8-11 for interior pages)
        uint32_t right_child = 0;
        if (page_size_ >= 12) {
            right_child = (static_cast<uint32_t>(page_data_[8]) << 24) |
                          (static_cast<uint32_t>(page_data_[9]) << 16) |
                          (static_cast<uint32_t>(page_data_[10]) << 8) |
                          static_cast<uint32_t>(page_data_[11]);
        }

        if (right_child == 0) {
            return false;
        }

        current_page_ = right_child;
        cell_index_ = -1;
    }
}

bool BTreeCursor::MoveToLeftmost() {
    if (!accessor_) {
        return false;
    }

    uint32_t root_page = accessor_->GetRootPage();
    if (root_page == 0) {
        eof_ = true;
        valid_ = false;
        return false;
    }

    Reset();
    return DescendToLeftmost(root_page);
}

bool BTreeCursor::MoveToRightmost() {
    if (!accessor_) {
        return false;
    }

    uint32_t root_page = accessor_->GetRootPage();
    if (root_page == 0) {
        eof_ = true;
        valid_ = false;
        return false;
    }

    Reset();
    return DescendToRightmost(root_page);
}

// ============================================================================
// Navigation - Ascend helpers
// ============================================================================

bool BTreeCursor::AscendAndMoveNext() {
    // Called when we've exhausted all cells in current leaf page
    // Pop from stack and move to next cell in parent

    while (!nav_stack_.empty()) {
        CursorStackEntry entry = nav_stack_.back();
        nav_stack_.pop_back();

        current_page_ = entry.page_num;
        cell_index_ = entry.cell_index;

        if (!ReadCurrentPage()) {
            return false;
        }

        int num_cells = GetNumCells();

        // Move to next cell in this interior page
        cell_index_++;

        if (cell_index_ < num_cells) {
            // There are more cells in this interior page
            // Push the new position and descend to leftmost of this child
            CursorStackEntry new_entry;
            new_entry.page_num = current_page_;
            new_entry.cell_index = cell_index_;
            nav_stack_.push_back(new_entry);

            // Get the left child of the current cell
            uint16_t cell_offset = GetCellOffset(cell_index_);
            uint32_t left_child = 0;

            if (IsTablePage()) {
                int64_t rowid;
                if (!DecodeTableInteriorCell(cell_offset, &left_child, &rowid)) {
                    return false;
                }
            } else {
                if (!DecodeIndexInteriorCell(cell_offset, &left_child)) {
                    return false;
                }
            }

            if (left_child == 0) {
                return false;
            }

            // Descend to leftmost of this subtree
            return DescendToLeftmost(left_child);
        }

        // We've exhausted this interior page too, continue ascending
        // The loop will pop the next entry from the stack
    }

    // No more entries - we've reached the end
    eof_ = true;
    valid_ = false;
    return false;
}

bool BTreeCursor::AscendAndMovePrev() {
    // Called when we've gone before the first cell in current leaf page
    // Pop from stack and move to prev cell in parent

    while (!nav_stack_.empty()) {
        CursorStackEntry entry = nav_stack_.back();
        nav_stack_.pop_back();

        current_page_ = entry.page_num;
        cell_index_ = entry.cell_index;

        if (!ReadCurrentPage()) {
            return false;
        }

        // Move to previous cell in this interior page
        cell_index_--;

        if (cell_index_ >= 0) {
            // There is a previous cell in this interior page
            // Get its left child and descend to rightmost of that subtree
            uint16_t cell_offset = GetCellOffset(cell_index_);
            uint32_t left_child = 0;

            if (IsTablePage()) {
                int64_t rowid;
                if (!DecodeTableInteriorCell(cell_offset, &left_child, &rowid)) {
                    return false;
                }
            } else {
                if (!DecodeIndexInteriorCell(cell_offset, &left_child)) {
                    return false;
                }
            }

            if (left_child == 0) {
                return false;
            }

            // Push current position and descend to rightmost of left_child
            CursorStackEntry new_entry;
            new_entry.page_num = current_page_;
            new_entry.cell_index = cell_index_;
            nav_stack_.push_back(new_entry);

            return DescendToRightmost(left_child);
        }

        // We need to take the rightmost child (pointer stored in header)
        // This is the case when cell_index_ was 0 and now is -1
        uint32_t right_child = 0;
        if (page_size_ >= 12) {
            right_child = (static_cast<uint32_t>(page_data_[8]) << 24) |
                          (static_cast<uint32_t>(page_data_[9]) << 16) |
                          (static_cast<uint32_t>(page_data_[10]) << 8) |
                          static_cast<uint32_t>(page_data_[11]);
        }

        if (right_child == 0) {
            return false;
        }

        // Push current position and descend to rightmost of right_child
        CursorStackEntry new_entry;
        new_entry.page_num = current_page_;
        new_entry.cell_index = -1;  // Indicates rightmost child
        nav_stack_.push_back(new_entry);

        return DescendToRightmost(right_child);
    }

    // No more entries - we've reached the beginning
    eof_ = true;
    valid_ = false;
    return false;
}

// ============================================================================
// Public Navigation API
// ============================================================================

bool BTreeCursor::First() {
    if (!accessor_) {
        return false;
    }

    Reset();
    valid_ = MoveToLeftmost();
    eof_ = !valid_;
    return valid_;
}

bool BTreeCursor::Last() {
    if (!accessor_) {
        return false;
    }

    Reset();
    valid_ = MoveToRightmost();
    eof_ = !valid_;
    return valid_;
}

bool BTreeCursor::Next() {
    if (eof_ || !valid_) {
        return false;
    }

    if (!ReadCurrentPage()) {
        eof_ = true;
        valid_ = false;
        return false;
    }

    svdb_assert_msg(IsLeafPage(), "Next() called on interior page");

    int num_cells = GetNumCells();
    cell_index_++;

    if (cell_index_ < num_cells) {
        // Still have cells in this leaf page
        valid_ = DecodeCurrentCell();
        if (!valid_) {
            eof_ = true;
        }
        return valid_;
    }

    // Exhausted this leaf, need to ascend
    return AscendAndMoveNext();
}

bool BTreeCursor::Prev() {
    if (eof_ || !valid_) {
        return false;
    }

    if (!ReadCurrentPage()) {
        eof_ = true;
        valid_ = false;
        return false;
    }

    svdb_assert_msg(IsLeafPage(), "Prev() called on interior page");

    cell_index_--;

    if (cell_index_ >= 0) {
        // Still have cells in this leaf page
        valid_ = DecodeCurrentCell();
        if (!valid_) {
            eof_ = true;
        }
        return valid_;
    }

    // Before first cell, need to ascend
    return AscendAndMovePrev();
}

bool BTreeCursor::Seek(const uint8_t* key, size_t key_len) {
    if (!accessor_ || !key || key_len == 0) {
        return false;
    }

    Reset();

    uint32_t root_page = accessor_->GetRootPage();
    if (root_page == 0) {
        eof_ = true;
        valid_ = false;
        return false;
    }

    current_page_ = root_page;

    // Navigate down to the leaf
    while (true) {
        if (!ReadCurrentPage()) {
            eof_ = true;
            valid_ = false;
            return false;
        }

        if (IsLeafPage()) {
            // Found leaf, do binary search for the key
            int num_cells = GetNumCells();
            if (num_cells == 0) {
                eof_ = true;
                valid_ = false;
                return false;
            }

            // Binary search for key
            int lo = 0, hi = num_cells - 1;
            int found_idx = -1;

            bool is_table = IsTablePage();
            int64_t search_rowid = 0;

            if (is_table) {
                int key_bytes;
                svdb_get_varint(key, key_len, &search_rowid, &key_bytes);
            }

            while (lo <= hi) {
                int mid = (lo + hi) / 2;
                uint16_t cell_offset = GetCellOffset(mid);

                int cmp = 0;
                if (is_table) {
                    // Compare rowids
                    int64_t cell_rowid = 0;
                    int64_t payload_len = 0;
                    int pn = 0, rn = 0;
                    svdb_get_varint(page_data_ + cell_offset, page_size_ - cell_offset, &payload_len, &pn);
                    if (pn > 0 && cell_offset + pn < page_size_) {
                        svdb_get_varint(page_data_ + cell_offset + pn, page_size_ - cell_offset - pn, &cell_rowid, &rn);
                    }
                    cmp = (cell_rowid < search_rowid) ? -1 : (cell_rowid > search_rowid) ? 1 : 0;
                } else {
                    // Compare keys
                    int64_t total_len, key_size;
                    int tn = 0, kn = 0;
                    svdb_get_varint(page_data_ + cell_offset, page_size_ - cell_offset, &total_len, &tn);
                    svdb_get_varint(page_data_ + cell_offset + tn, page_size_ - cell_offset - tn, &key_size, &kn);

                    size_t cmp_len = (key_len < static_cast<size_t>(key_size)) ? key_len : static_cast<size_t>(key_size);
                    cmp = std::memcmp(key, page_data_ + cell_offset + tn + kn, cmp_len);
                    if (cmp == 0) {
                        cmp = (key_len < static_cast<size_t>(key_size)) ? -1 : (key_len > static_cast<size_t>(key_size)) ? 1 : 0;
                    }
                }

                if (cmp == 0) {
                    found_idx = mid;
                    break;
                } else if (cmp < 0) {
                    lo = mid + 1;
                } else {
                    hi = mid - 1;
                }
            }

            if (found_idx >= 0) {
                cell_index_ = found_idx;
                valid_ = DecodeCurrentCell();
                eof_ = !valid_;
                return valid_;
            }

            // Key not found - position at first key >= search key
            cell_index_ = lo;
            if (cell_index_ >= num_cells) {
                // Position past end
                eof_ = true;
                valid_ = false;
            } else {
                valid_ = DecodeCurrentCell();
                eof_ = !valid_;
            }
            return valid_;
        }

        // Interior page - find the child to descend into
        int num_cells = GetNumCells();

        // Binary search for the right child
        int child_idx = num_cells;  // Default to rightmost
        bool is_table = IsTablePage();
        int64_t search_rowid = 0;

        if (is_table) {
            int key_bytes;
            svdb_get_varint(key, key_len, &search_rowid, &key_bytes);
        }

        for (int i = 0; i < num_cells; i++) {
            uint16_t cell_offset = GetCellOffset(i);
            int cmp = 0;

            if (is_table) {
                int64_t cell_rowid = 0;
                int64_t left_child_64 = 0;
                // Read left child (4 bytes) then rowid
                if (cell_offset + 4 <= page_size_) {
                    // Skip left child pointer
                    int rn = 0;
                    svdb_get_varint(page_data_ + cell_offset + 4, page_size_ - cell_offset - 4, &cell_rowid, &rn);
                }
                cmp = (cell_rowid < search_rowid) ? -1 : (cell_rowid > search_rowid) ? 1 : 0;
            } else {
                // Index interior: compare keys
                // Format: left_child(4) + payload_len(varint) + key
                int64_t total_len, key_size;
                int tn = 0, kn = 0;
                int offset = cell_offset + 4;  // Skip left child
                svdb_get_varint(page_data_ + offset, page_size_ - offset, &total_len, &tn);
                svdb_get_varint(page_data_ + offset + tn, page_size_ - offset - tn, &key_size, &kn);

                size_t cmp_len = (key_len < static_cast<size_t>(key_size)) ? key_len : static_cast<size_t>(key_size);
                cmp = std::memcmp(key, page_data_ + offset + tn + kn, cmp_len);
                if (cmp == 0) {
                    cmp = (key_len < static_cast<size_t>(key_size)) ? -1 : (key_len > static_cast<size_t>(key_size)) ? 1 : 0;
                }
            }

            if (cmp >= 0) {
                child_idx = i;
                break;
            }
        }

        // Push current position onto stack
        CursorStackEntry entry;
        entry.page_num = current_page_;
        entry.cell_index = child_idx;
        nav_stack_.push_back(entry);

        // Get the child page
        uint32_t child_page = 0;
        if (child_idx < num_cells) {
            uint16_t cell_offset = GetCellOffset(child_idx);
            if (cell_offset + 4 <= page_size_) {
                child_page = (static_cast<uint32_t>(page_data_[cell_offset]) << 24) |
                             (static_cast<uint32_t>(page_data_[cell_offset + 1]) << 16) |
                             (static_cast<uint32_t>(page_data_[cell_offset + 2]) << 8) |
                             static_cast<uint32_t>(page_data_[cell_offset + 3]);
            }
        } else {
            // Rightmost child
            if (page_size_ >= 12) {
                child_page = (static_cast<uint32_t>(page_data_[8]) << 24) |
                             (static_cast<uint32_t>(page_data_[9]) << 16) |
                             (static_cast<uint32_t>(page_data_[10]) << 8) |
                             static_cast<uint32_t>(page_data_[11]);
            }
        }

        if (child_page == 0) {
            eof_ = true;
            valid_ = false;
            return false;
        }

        current_page_ = child_page;
    }
}

// ============================================================================
// PageCache Implementation
// ============================================================================

PageCache::PageCache(size_t max_pages)
    : max_pages_(max_pages), cache_size_(0), hits_(0), misses_(0) {
}

PageCache::~PageCache() {
    Clear();
}

BTreePage* PageCache::GetPage(uint32_t page_num) {
    // TODO: Implement actual page cache with hash table
    (void)page_num;
    misses_++;
    return nullptr;
}

void PageCache::PutPage(uint32_t page_num, BTreePage* page) {
    // TODO: Implement actual page cache with hash table
    (void)page_num;
    (void)page;
}

void PageCache::Invalidate(uint32_t page_num) {
    // TODO: Implement actual page cache invalidation
    (void)page_num;
}

void PageCache::Clear() {
    cache_size_ = 0;
}

} // namespace ds
} // namespace svdb

// ============================================================================
// C-compatible wrapper functions
// ============================================================================

extern "C" {

void* SVDB_DS_BTreeCursor_Create() {
    return new svdb::ds::BTreeCursor();
}

void SVDB_DS_BTreeCursor_Destroy(void* cursor) {
    delete static_cast<svdb::ds::BTreeCursor*>(cursor);
}

void SVDB_DS_BTreeCursor_Reset(void* cursor) {
    auto* c = static_cast<svdb::ds::BTreeCursor*>(cursor);
    c->Reset();
}

int SVDB_DS_BTreeCursor_First(void* cursor) {
    auto* c = static_cast<svdb::ds::BTreeCursor*>(cursor);
    return c->First() ? 1 : 0;
}

int SVDB_DS_BTreeCursor_Last(void* cursor) {
    auto* c = static_cast<svdb::ds::BTreeCursor*>(cursor);
    return c->Last() ? 1 : 0;
}

int SVDB_DS_BTreeCursor_Next(void* cursor) {
    auto* c = static_cast<svdb::ds::BTreeCursor*>(cursor);
    return c->Next() ? 1 : 0;
}

int SVDB_DS_BTreeCursor_Prev(void* cursor) {
    auto* c = static_cast<svdb::ds::BTreeCursor*>(cursor);
    return c->Prev() ? 1 : 0;
}

int SVDB_DS_BTreeCursor_Seek(void* cursor, const uint8_t* key, size_t key_len) {
    auto* c = static_cast<svdb::ds::BTreeCursor*>(cursor);
    return c->Seek(key, key_len) ? 1 : 0;
}

int SVDB_DS_BTreeCursor_IsValid(void* cursor) {
    auto* c = static_cast<svdb::ds::BTreeCursor*>(cursor);
    return c->IsValid() ? 1 : 0;
}

int SVDB_DS_BTreeCursor_IsEof(void* cursor) {
    auto* c = static_cast<svdb::ds::BTreeCursor*>(cursor);
    return c->IsEof() ? 1 : 0;
}

const uint8_t* SVDB_DS_BTreeCursor_GetKey(void* cursor, size_t* out_len) {
    auto* c = static_cast<svdb::ds::BTreeCursor*>(cursor);
    *out_len = c->GetKeyLen();
    return c->GetKey();
}

const uint8_t* SVDB_DS_BTreeCursor_GetValue(void* cursor, size_t* out_len) {
    auto* c = static_cast<svdb::ds::BTreeCursor*>(cursor);
    *out_len = c->GetValueLen();
    return c->GetValue();
}

int64_t SVDB_DS_BTreeCursor_GetRowid(void* cursor) {
    auto* c = static_cast<svdb::ds::BTreeCursor*>(cursor);
    return c->GetRowid();
}

void* SVDB_DS_PageCache_Create(size_t max_pages) {
    return new svdb::ds::PageCache(max_pages);
}

void SVDB_DS_PageCache_Destroy(void* cache) {
    delete static_cast<svdb::ds::PageCache*>(cache);
}

void SVDB_DS_PageCache_Clear(void* cache) {
    auto* c = static_cast<svdb::ds::PageCache*>(cache);
    c->Clear();
}

size_t SVDB_DS_PageCache_GetSize(void* cache) {
    auto* c = static_cast<svdb::ds::PageCache*>(cache);
    return c->GetSize();
}

size_t SVDB_DS_PageCache_GetHits(void* cache) {
    auto* c = static_cast<svdb::ds::PageCache*>(cache);
    return c->GetHits();
}

size_t SVDB_DS_PageCache_GetMisses(void* cache) {
    auto* c = static_cast<svdb::ds::PageCache*>(cache);
    return c->GetMisses();
}

} // extern "C"