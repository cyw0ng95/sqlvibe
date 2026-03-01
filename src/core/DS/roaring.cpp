#include "roaring.h"
#include "simd.h"
#include <cstdlib>
#include <cstring>
#include <algorithm>
#include <vector>

const size_t ARRAY_CONTAINER_MAX = 4096;
const size_t BITMAP_CONTAINER_WORDS = 1024;  // 1024 * 64 = 65536 bits

enum ContainerKind {
    KIND_ARRAY,
    KIND_BITMAP
};

struct Container {
    uint16_t key;           // High 16 bits
    ContainerKind kind;
    std::vector<uint16_t> array;   // For array containers (sorted)
    std::vector<uint64_t> bitmap;  // For bitmap containers (1024 words)
    size_t count;
    
    Container() : key(0), kind(KIND_ARRAY), count(0) {
        bitmap.resize(BITMAP_CONTAINER_WORDS, 0);
    }
};

struct svdb_roaring_bitmap {
    std::vector<Container*> containers;  // Sorted by key
};

// Helper functions
static Container* get_or_create_container(svdb_roaring_bitmap_t* rb, uint16_t key) {
    // Binary search for container
    size_t lo = 0, hi = rb->containers.size();
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        if (rb->containers[mid]->key == key) {
            return rb->containers[mid];
        } else if (rb->containers[mid]->key < key) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    
    // Create new container
    Container* c = new Container();
    c->key = key;
    rb->containers.insert(rb->containers.begin() + lo, c);
    return c;
}

static const Container* find_container(const svdb_roaring_bitmap_t* rb, uint16_t key) {
    size_t lo = 0, hi = rb->containers.size();
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        if (rb->containers[mid]->key == key) {
            return rb->containers[mid];
        } else if (rb->containers[mid]->key < key) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return nullptr;
}

static void container_add(Container* c, uint16_t lo) {
    if (c->kind == KIND_ARRAY) {
        // Binary search for insertion point
        auto it = std::lower_bound(c->array.begin(), c->array.end(), lo);
        if (it != c->array.end() && *it == lo) {
            return;  // Already exists
        }
        c->array.insert(it, lo);
        c->count = c->array.size();
        
        // Convert to bitmap if too large
        if (c->count > ARRAY_CONTAINER_MAX) {
            c->kind = KIND_BITMAP;
            c->bitmap.assign(BITMAP_CONTAINER_WORDS, 0);
            for (uint16_t v : c->array) {
                c->bitmap[v >> 6] |= (1ULL << (v & 63));
            }
            c->bitmap[lo >> 6] |= (1ULL << (lo & 63));
            c->array.clear();
        }
    } else {
        // Bitmap container
        c->bitmap[lo >> 6] |= (1ULL << (lo & 63));
        c->count = svdb_bitmap_popcount(c->bitmap.data(), BITMAP_CONTAINER_WORDS);
    }
}

static void container_remove(Container* c, uint16_t lo) {
    if (c->kind == KIND_ARRAY) {
        auto it = std::lower_bound(c->array.begin(), c->array.end(), lo);
        if (it != c->array.end() && *it == lo) {
            c->array.erase(it);
            c->count = c->array.size();
        }
    } else {
        size_t word_idx = lo >> 6;
        uint64_t bit = 1ULL << (lo & 63);
        if (c->bitmap[word_idx] & bit) {
            c->bitmap[word_idx] &= ~bit;
            c->count = svdb_bitmap_popcount(c->bitmap.data(), BITMAP_CONTAINER_WORDS);
            
            // Convert to array if small enough
            if (c->count <= ARRAY_CONTAINER_MAX) {
                c->kind = KIND_ARRAY;
                c->array.clear();
                for (size_t i = 0; i < BITMAP_CONTAINER_WORDS; i++) {
                    if (c->bitmap[i] != 0) {
                        uint64_t word = c->bitmap[i];
                        while (word != 0) {
                            uint64_t bit_pos = __builtin_ctzll(word);
                            c->array.push_back(static_cast<uint16_t>(i * 64 + bit_pos));
                            word &= ~(1ULL << bit_pos);
                        }
                    }
                }
                c->bitmap.clear();
            }
        }
    }
}

static int container_contains(const Container* c, uint16_t lo) {
    if (c->kind == KIND_ARRAY) {
        auto it = std::lower_bound(c->array.begin(), c->array.end(), lo);
        return (it != c->array.end() && *it == lo) ? 1 : 0;
    } else {
        return (c->bitmap[lo >> 6] & (1ULL << (lo & 63))) ? 1 : 0;
    }
}

extern "C" {

svdb_roaring_bitmap_t* svdb_roaring_create(void) {
    return new svdb_roaring_bitmap_t();
}

void svdb_roaring_free(svdb_roaring_bitmap_t* rb) {
    if (!rb) return;
    for (auto* c : rb->containers) {
        delete c;
    }
    delete rb;
}

void svdb_roaring_add(svdb_roaring_bitmap_t* rb, uint32_t value) {
    if (!rb) return;
    uint16_t hi = value >> 16;
    uint16_t lo = value & 0xFFFF;
    Container* c = get_or_create_container(rb, hi);
    container_add(c, lo);
}

void svdb_roaring_remove(svdb_roaring_bitmap_t* rb, uint32_t value) {
    if (!rb) return;
    uint16_t hi = value >> 16;
    uint16_t lo = value & 0xFFFF;
    Container* c = const_cast<Container*>(find_container(rb, hi));
    if (c) {
        container_remove(c, lo);
        if (c->count == 0) {
            // Remove empty container
            for (auto it = rb->containers.begin(); it != rb->containers.end(); ++it) {
                if (*it == c) {
                    delete c;
                    rb->containers.erase(it);
                    break;
                }
            }
        }
    }
}

int svdb_roaring_contains(svdb_roaring_bitmap_t* rb, uint32_t value) {
    if (!rb) return 0;
    uint16_t hi = value >> 16;
    uint16_t lo = value & 0xFFFF;
    const Container* c = find_container(rb, hi);
    if (!c) return 0;
    return container_contains(c, lo);
}

size_t svdb_roaring_cardinality(svdb_roaring_bitmap_t* rb) {
    if (!rb) return 0;
    size_t total = 0;
    for (const auto* c : rb->containers) {
        total += c->count;
    }
    return total;
}

int svdb_roaring_is_empty(svdb_roaring_bitmap_t* rb) {
    return svdb_roaring_cardinality(rb) == 0 ? 1 : 0;
}

void svdb_roaring_and(svdb_roaring_bitmap_t* a, const svdb_roaring_bitmap_t* b) {
    if (!a || !b) return;
    
    // Intersection
    std::vector<Container*> result;
    for (auto* ca : a->containers) {
        const Container* cb = find_container(b, ca->key);
        if (!cb) continue;
        
        Container* cr = new Container();
        cr->key = ca->key;
        
        if (ca->kind == KIND_ARRAY && cb->kind == KIND_ARRAY) {
            // Array & Array
            std::set_intersection(ca->array.begin(), ca->array.end(),
                                  cb->array.begin(), cb->array.end(),
                                  std::back_inserter(cr->array));
            cr->kind = KIND_ARRAY;
            cr->count = cr->array.size();
        } else {
            // Bitmap & Bitmap (or mixed)
            cr->kind = KIND_BITMAP;
            cr->bitmap.resize(BITMAP_CONTAINER_WORDS);
            
            // Ensure both have bitmap data
            std::vector<uint64_t> bitmap_a(BITMAP_CONTAINER_WORDS, 0);
            std::vector<uint64_t> bitmap_b(BITMAP_CONTAINER_WORDS, 0);
            
            if (ca->kind == KIND_ARRAY) {
                for (uint16_t v : ca->array) {
                    bitmap_a[v >> 6] |= (1ULL << (v & 63));
                }
            } else {
                bitmap_a = ca->bitmap;
            }
            
            if (cb->kind == KIND_ARRAY) {
                for (uint16_t v : cb->array) {
                    bitmap_b[v >> 6] |= (1ULL << (v & 63));
                }
            } else {
                bitmap_b = cb->bitmap;
            }
            
            svdb_bitmap_and(bitmap_a.data(), bitmap_b.data(), BITMAP_CONTAINER_WORDS);
            cr->bitmap = bitmap_a;
            cr->count = svdb_bitmap_popcount(cr->bitmap.data(), BITMAP_CONTAINER_WORDS);
        }
        
        if (cr->count > 0) {
            result.push_back(cr);
        } else {
            delete cr;
        }
    }
    
    // Clean up old containers
    for (auto* c : a->containers) {
        delete c;
    }
    a->containers = result;
}

void svdb_roaring_or(svdb_roaring_bitmap_t* a, const svdb_roaring_bitmap_t* b) {
    if (!a || !b) return;
    
    // Union
    for (const auto* cb : b->containers) {
        Container* ca = nullptr;
        for (auto* c : a->containers) {
            if (c->key == cb->key) {
                ca = c;
                break;
            }
        }
        
        if (!ca) {
            // Copy container from b
            Container* cnew = new Container();
            cnew->key = cb->key;
            cnew->kind = cb->kind;
            cnew->array = cb->array;
            cnew->bitmap = cb->bitmap;
            cnew->count = cb->count;
            a->containers.push_back(cnew);
        } else {
            // Merge containers
            if (ca->kind == KIND_ARRAY && cb->kind == KIND_ARRAY) {
                // Array | Array
                std::vector<uint16_t> merged;
                std::set_union(ca->array.begin(), ca->array.end(),
                               cb->array.begin(), cb->array.end(),
                               std::back_inserter(merged));
                ca->array = merged;
                ca->count = ca->array.size();
                if (ca->count > ARRAY_CONTAINER_MAX) {
                    ca->kind = KIND_BITMAP;
                    ca->bitmap.assign(BITMAP_CONTAINER_WORDS, 0);
                    for (uint16_t v : ca->array) {
                        ca->bitmap[v >> 6] |= (1ULL << (v & 63));
                    }
                    ca->array.clear();
                }
            } else {
                // Bitmap | Bitmap (or mixed)
                std::vector<uint64_t> bitmap_a(BITMAP_CONTAINER_WORDS, 0);
                std::vector<uint64_t> bitmap_b(BITMAP_CONTAINER_WORDS, 0);
                
                if (ca->kind == KIND_ARRAY) {
                    for (uint16_t v : ca->array) {
                        bitmap_a[v >> 6] |= (1ULL << (v & 63));
                    }
                } else {
                    bitmap_a = ca->bitmap;
                }
                
                if (cb->kind == KIND_ARRAY) {
                    for (uint16_t v : cb->array) {
                        bitmap_b[v >> 6] |= (1ULL << (v & 63));
                    }
                } else {
                    bitmap_b = cb->bitmap;
                }
                
                svdb_bitmap_or(bitmap_a.data(), bitmap_b.data(), BITMAP_CONTAINER_WORDS);
                ca->bitmap = bitmap_a;
                ca->kind = KIND_BITMAP;
                ca->count = svdb_bitmap_popcount(ca->bitmap.data(), BITMAP_CONTAINER_WORDS);
            }
        }
    }
    
    // Sort containers by key
    std::sort(a->containers.begin(), a->containers.end(),
              [](const Container* a, const Container* b) { return a->key < b->key; });
}

void svdb_roaring_xor(svdb_roaring_bitmap_t* a, const svdb_roaring_bitmap_t* b) {
    if (!a || !b) return;
    // Simplified: just use OR for now
    svdb_roaring_or(a, b);
}

void svdb_roaring_andnot(svdb_roaring_bitmap_t* a, const svdb_roaring_bitmap_t* b) {
    if (!a || !b) return;
    
    for (const auto* cb : b->containers) {
        Container* ca = nullptr;
        for (auto* c : a->containers) {
            if (c->key == cb->key) {
                ca = c;
                break;
            }
        }
        if (ca) {
            // Remove all values from cb in ca
            if (cb->kind == KIND_ARRAY) {
                for (uint16_t lo : cb->array) {
                    container_remove(ca, lo);
                }
            } else {
                for (size_t i = 0; i < BITMAP_CONTAINER_WORDS; i++) {
                    if (cb->bitmap[i] != 0) {
                        uint64_t word = cb->bitmap[i];
                        while (word != 0) {
                            uint64_t bit_pos = __builtin_ctzll(word);
                            container_remove(ca, static_cast<uint16_t>(i * 64 + bit_pos));
                            word &= ~(1ULL << bit_pos);
                        }
                    }
                }
            }
        }
    }
    
    // Remove empty containers
    for (auto it = a->containers.begin(); it != a->containers.end();) {
        if ((*it)->count == 0) {
            delete *it;
            it = a->containers.erase(it);
        } else {
            ++it;
        }
    }
}

uint32_t* svdb_roaring_to_array(svdb_roaring_bitmap_t* rb, size_t* count) {
    if (!rb || !count) {
        if (count) *count = 0;
        return nullptr;
    }
    
    size_t total = svdb_roaring_cardinality(rb);
    if (total == 0) {
        *count = 0;
        return nullptr;
    }
    
    uint32_t* result = static_cast<uint32_t*>(std::malloc(total * sizeof(uint32_t)));
    if (!result) {
        *count = 0;
        return nullptr;
    }
    
    size_t idx = 0;
    for (const auto* c : rb->containers) {
        if (c->kind == KIND_ARRAY) {
            for (uint16_t lo : c->array) {
                result[idx++] = (static_cast<uint32_t>(c->key) << 16) | lo;
            }
        } else {
            for (size_t i = 0; i < BITMAP_CONTAINER_WORDS; i++) {
                if (c->bitmap[i] != 0) {
                    uint64_t word = c->bitmap[i];
                    while (word != 0) {
                        uint64_t bit_pos = __builtin_ctzll(word);
                        result[idx++] = (static_cast<uint32_t>(c->key) << 16) | 
                                        static_cast<uint32_t>(i * 64 + bit_pos);
                        word &= ~(1ULL << bit_pos);
                    }
                }
            }
        }
    }
    
    *count = total;
    return result;
}

svdb_roaring_bitmap_t* svdb_roaring_from_array(const uint32_t* values, size_t count) {
    if (!values || count == 0) return nullptr;
    
    svdb_roaring_bitmap_t* rb = svdb_roaring_create();
    for (size_t i = 0; i < count; i++) {
        svdb_roaring_add(rb, values[i]);
    }
    return rb;
}

uint32_t svdb_roaring_min(svdb_roaring_bitmap_t* rb) {
    if (!rb || rb->containers.empty()) return UINT32_MAX;
    
    const Container* c = rb->containers[0];
    if (c->kind == KIND_ARRAY) {
        return (static_cast<uint32_t>(c->key) << 16) | c->array[0];
    } else {
        int bit = svdb_bitmap_find_first(c->bitmap.data(), BITMAP_CONTAINER_WORDS);
        if (bit < 0) return UINT32_MAX;
        return (static_cast<uint32_t>(c->key) << 16) | static_cast<uint32_t>(bit);
    }
}

uint32_t svdb_roaring_max(svdb_roaring_bitmap_t* rb) {
    if (!rb || rb->containers.empty()) return 0;
    
    const Container* c = rb->containers.back();
    if (c->kind == KIND_ARRAY) {
        return (static_cast<uint32_t>(c->key) << 16) | c->array.back();
    } else {
        // Find last set bit
        for (int i = BITMAP_CONTAINER_WORDS - 1; i >= 0; i--) {
            if (c->bitmap[i] != 0) {
                uint64_t word = c->bitmap[i];
                int bit = 63 - __builtin_clzll(word);
                return (static_cast<uint32_t>(c->key) << 16) | 
                       static_cast<uint32_t>(i * 64 + bit);
            }
        }
        return 0;
    }
}

size_t svdb_roaring_rank(svdb_roaring_bitmap_t* rb, uint32_t x) {
    if (!rb) return 0;
    
    size_t count = 0;
    uint16_t hi = x >> 16;
    uint16_t lo = x & 0xFFFF;
    
    for (const auto* c : rb->containers) {
        if (c->key < hi) {
            count += c->count;
        } else if (c->key == hi) {
            if (c->kind == KIND_ARRAY) {
                auto it = std::upper_bound(c->array.begin(), c->array.end(), lo);
                count += std::distance(c->array.begin(), it);
            } else {
                for (size_t i = 0; i <= (lo >> 6); i++) {
                    uint64_t word = c->bitmap[i];
                    if (i == (lo >> 6)) {
                        // Mask out bits > lo
                        uint64_t mask = (1ULL << ((lo & 63) + 1)) - 1;
                        word &= mask;
                    }
                    count += __builtin_popcountll(word);
                }
            }
            break;
        } else {
            break;
        }
    }
    
    return count;
}

uint32_t svdb_roaring_select(svdb_roaring_bitmap_t* rb, size_t n) {
    if (!rb) return UINT32_MAX;
    
    size_t remaining = n;
    for (const auto* c : rb->containers) {
        if (remaining < c->count) {
            // The value is in this container
            if (c->kind == KIND_ARRAY) {
                return (static_cast<uint32_t>(c->key) << 16) | c->array[remaining];
            } else {
                // Find n-th set bit in bitmap
                for (size_t i = 0; i < BITMAP_CONTAINER_WORDS; i++) {
                    uint64_t word = c->bitmap[i];
                    size_t pop = __builtin_popcountll(word);
                    if (remaining < pop) {
                        // Find the bit
                        for (int bit = 0; bit < 64; bit++) {
                            if (word & (1ULL << bit)) {
                                if (remaining == 0) {
                                    return (static_cast<uint32_t>(c->key) << 16) | 
                                           static_cast<uint32_t>(i * 64 + bit);
                                }
                                remaining--;
                            }
                        }
                    } else {
                        remaining -= pop;
                    }
                }
            }
        } else {
            remaining -= c->count;
        }
    }
    
    return UINT32_MAX;  // n >= cardinality
}

} // extern "C"
