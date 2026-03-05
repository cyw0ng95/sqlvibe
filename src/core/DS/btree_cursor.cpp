#include "btree_cursor.h"
#include <algorithm>
#include <cstring>

namespace svdb {
namespace ds {

BTreeCursor::BTreeCursor() : btree_(nullptr), valid_(false) {
}

BTreeCursor::~BTreeCursor() {
}

void BTreeCursor::Reset() {
    valid_ = false;
    current_key_.clear();
    current_value_.clear();
    while (!page_stack_.empty()) page_stack_.pop();
}

bool BTreeCursor::First() {
    Reset();
    return Next();
}

bool BTreeCursor::Last() {
    Reset();
    return Prev();
}

bool BTreeCursor::Next() {
    return valid_;
}

bool BTreeCursor::Prev() {
    return valid_;
}

bool BTreeCursor::Seek(const uint8_t* key, size_t key_len) {
    current_key_.assign(key, key + key_len);
    valid_ = !current_key_.empty();
    return valid_;
}

PageCache::PageCache(size_t max_pages) 
    : max_pages_(max_pages), cache_size_(0), hits_(0), misses_(0) {
}

PageCache::~PageCache() {
    Clear();
}

BTreePage* PageCache::GetPage(uint32_t page_num) {
    return nullptr;
}

void PageCache::PutPage(uint32_t page_num, BTreePage* page) {
}

void PageCache::Invalidate(uint32_t page_num) {
}

void PageCache::Clear() {
    cache_size_ = 0;
}

} // namespace ds
} // namespace svdb

// C-compatible wrapper functions
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

}
