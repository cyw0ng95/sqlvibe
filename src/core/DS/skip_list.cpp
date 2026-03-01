#include "skip_list.h"
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <random>
#include <vector>
#include <map>
#include <string>

/* -------------------------------------------------------------------------
 * Internal implementation
 * ---------------------------------------------------------------------- */

static constexpr int SKIP_MAX_LEVEL = 16;

/* A variant key type: either int64 or byte-string. */
struct SkipKey {
    bool is_int;
    int64_t int_val;
    std::string str_val;

    bool operator<(const SkipKey& o) const {
        if (is_int != o.is_int) return is_int < o.is_int; /* ints sort before strings */
        if (is_int) return int_val < o.int_val;
        return str_val < o.str_val;
    }
    bool operator==(const SkipKey& o) const {
        if (is_int != o.is_int) return false;
        if (is_int) return int_val == o.int_val;
        return str_val == o.str_val;
    }
};

struct SkipNode {
    SkipKey key;
    std::vector<int64_t> row_indices; /* all row indices for this key */
    SkipNode* forward[SKIP_MAX_LEVEL];
    int level;

    SkipNode(const SkipKey& k, int lvl) : key(k), level(lvl) {
        for (int i = 0; i < SKIP_MAX_LEVEL; i++) forward[i] = nullptr;
    }
};

struct SkipList {
    SkipNode* head;
    int cur_level;
    int unique_keys;
    std::mt19937 rng;

    SkipList() : cur_level(0), unique_keys(0), rng(std::random_device{}()) {
        SkipKey sentinel;
        sentinel.is_int  = true;
        sentinel.int_val = INT64_MIN;
        head = new SkipNode(sentinel, SKIP_MAX_LEVEL);
    }

    ~SkipList() {
        SkipNode* cur = head;
        while (cur) {
            SkipNode* nxt = cur->forward[0];
            delete cur;
            cur = nxt;
        }
    }

    int random_level() {
        int lvl = 1;
        while (lvl < SKIP_MAX_LEVEL && (rng() & 1)) lvl++;
        return lvl;
    }

    /* Find the node exactly matching key, or nullptr. */
    SkipNode* find(const SkipKey& key) {
        SkipNode* cur = head;
        for (int i = cur_level - 1; i >= 0; i--) {
            while (cur->forward[i] && cur->forward[i]->key < key)
                cur = cur->forward[i];
        }
        cur = cur->forward[0];
        if (cur && cur->key == key) return cur;
        return nullptr;
    }

    void insert(const SkipKey& key, int64_t row_idx) {
        SkipNode* update[SKIP_MAX_LEVEL];
        SkipNode* cur = head;
        for (int i = cur_level - 1; i >= 0; i--) {
            while (cur->forward[i] && cur->forward[i]->key < key)
                cur = cur->forward[i];
            update[i] = cur;
        }
        cur = cur->forward[0];

        if (cur && cur->key == key) {
            /* Key exists: add row_idx if not already present. */
            for (int64_t v : cur->row_indices) if (v == row_idx) return;
            cur->row_indices.push_back(row_idx);
            return;
        }

        /* New key. */
        int lvl = random_level();
        if (lvl > cur_level) {
            for (int i = cur_level; i < lvl; i++) update[i] = head;
            cur_level = lvl;
        }

        SkipNode* node = new SkipNode(key, lvl);
        node->row_indices.push_back(row_idx);

        for (int i = 0; i < lvl; i++) {
            node->forward[i]    = update[i]->forward[i];
            update[i]->forward[i] = node;
        }
        unique_keys++;
    }

    void remove(const SkipKey& key, int64_t row_idx) {
        SkipNode* update[SKIP_MAX_LEVEL];
        SkipNode* cur = head;
        for (int i = cur_level - 1; i >= 0; i--) {
            while (cur->forward[i] && cur->forward[i]->key < key)
                cur = cur->forward[i];
            update[i] = cur;
        }
        cur = cur->forward[0];
        if (!cur || !(cur->key == key)) return;

        /* Remove row_idx from the node's list. */
        auto& v = cur->row_indices;
        for (auto it = v.begin(); it != v.end(); ++it) {
            if (*it == row_idx) { v.erase(it); break; }
        }

        if (!v.empty()) return; /* Other rows still use this key. */

        /* Remove the node entirely. */
        for (int i = 0; i < cur_level; i++) {
            if (update[i]->forward[i] != cur) break;
            update[i]->forward[i] = cur->forward[i];
        }
        delete cur;
        unique_keys--;

        while (cur_level > 1 && !head->forward[cur_level - 1])
            cur_level--;
    }
};

/* -------------------------------------------------------------------------
 * C API
 * ---------------------------------------------------------------------- */

extern "C" {

svdb_skiplist_t svdb_skiplist_create(void) {
    return static_cast<svdb_skiplist_t>(new SkipList());
}

void svdb_skiplist_destroy(svdb_skiplist_t sl) {
    if (sl) delete static_cast<SkipList*>(sl);
}

void svdb_skiplist_insert_int(svdb_skiplist_t sl, int64_t key, int64_t row_idx) {
    if (!sl) return;
    SkipKey k; k.is_int = true; k.int_val = key;
    static_cast<SkipList*>(sl)->insert(k, row_idx);
}

void svdb_skiplist_insert_str(svdb_skiplist_t sl,
                               const uint8_t* key, size_t key_len,
                               int64_t row_idx) {
    if (!sl || !key) return;
    SkipKey k; k.is_int = false;
    k.str_val.assign(reinterpret_cast<const char*>(key), key_len);
    static_cast<SkipList*>(sl)->insert(k, row_idx);
}

void svdb_skiplist_delete_int(svdb_skiplist_t sl, int64_t key, int64_t row_idx) {
    if (!sl) return;
    SkipKey k; k.is_int = true; k.int_val = key;
    static_cast<SkipList*>(sl)->remove(k, row_idx);
}

void svdb_skiplist_delete_str(svdb_skiplist_t sl,
                               const uint8_t* key, size_t key_len,
                               int64_t row_idx) {
    if (!sl || !key) return;
    SkipKey k; k.is_int = false;
    k.str_val.assign(reinterpret_cast<const char*>(key), key_len);
    static_cast<SkipList*>(sl)->remove(k, row_idx);
}

int svdb_skiplist_find_int(svdb_skiplist_t sl, int64_t key,
                            int64_t* out_indices, int max_out) {
    if (!sl || !out_indices || max_out <= 0) return 0;
    SkipKey k; k.is_int = true; k.int_val = key;
    SkipNode* node = static_cast<SkipList*>(sl)->find(k);
    if (!node) return 0;
    int count = 0;
    for (int64_t v : node->row_indices) {
        if (count >= max_out) break;
        out_indices[count++] = v;
    }
    return count;
}

int svdb_skiplist_find_str(svdb_skiplist_t sl,
                            const uint8_t* key, size_t key_len,
                            int64_t* out_indices, int max_out) {
    if (!sl || !key || !out_indices || max_out <= 0) return 0;
    SkipKey k; k.is_int = false;
    k.str_val.assign(reinterpret_cast<const char*>(key), key_len);
    SkipNode* node = static_cast<SkipList*>(sl)->find(k);
    if (!node) return 0;
    int count = 0;
    for (int64_t v : node->row_indices) {
        if (count >= max_out) break;
        out_indices[count++] = v;
    }
    return count;
}

int svdb_skiplist_range_int(svdb_skiplist_t sl, int64_t lo, int64_t hi, int inclusive,
                             int64_t* out_indices, int max_out) {
    if (!sl || !out_indices || max_out <= 0) return 0;
    SkipList* list = static_cast<SkipList*>(sl);

    /* Walk from the first node >= lo (or > lo if exclusive). */
    SkipKey lo_key; lo_key.is_int = true; lo_key.int_val = lo;
    SkipNode* cur = list->head;
    for (int i = list->cur_level - 1; i >= 0; i--) {
        while (cur->forward[i] && cur->forward[i]->key < lo_key)
            cur = cur->forward[i];
    }
    cur = cur->forward[0];

    int count = 0;
    while (cur && count < max_out) {
        if (!cur->key.is_int) break;
        int64_t v = cur->key.int_val;
        if (inclusive) {
            if (v < lo || v > hi) { cur = cur->forward[0]; continue; }
        } else {
            if (v <= lo || v >= hi) {
                if (v >= hi) break;
                cur = cur->forward[0];
                continue;
            }
        }
        if (v > hi) break;
        for (int64_t ri : cur->row_indices) {
            if (count >= max_out) break;
            out_indices[count++] = ri;
        }
        cur = cur->forward[0];
    }
    return count;
}

int svdb_skiplist_len(svdb_skiplist_t sl) {
    if (!sl) return 0;
    return static_cast<SkipList*>(sl)->unique_keys;
}

} /* extern "C" */
