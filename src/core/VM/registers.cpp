#include "registers.h"

namespace svdb {

RegisterAllocator::RegisterAllocator(int initial_regs)
    : max_reg_(initial_regs > 0 ? initial_regs : 16)
    , next_reg_(0)
    , allocated_(0)
{
}

int RegisterAllocator::alloc() {
    // Fast path: check registers 0-63 using bitmap
    if (next_reg_ < 64) {
        for (int i = next_reg_; i < 64; i++) {
            if ((allocated_ & (1ULL << i)) == 0) {
                allocated_ |= (1ULL << i);
                next_reg_ = i;
                return i;
            }
        }
    }
    
    // Slow path: check registers >= 64
    for (int i = 64; ; i++) {
        if (large_regs_.find(i) == large_regs_.end()) {
            large_regs_.insert(i);
            if (i >= max_reg_) {
                max_reg_ = i + 1;
            }
            return i;
        }
    }
}

void RegisterAllocator::alloc_many(int count, int* result) {
    for (int i = 0; i < count; i++) {
        result[i] = alloc();
    }
}

void RegisterAllocator::release(int reg) {
    if (reg < 0) return;
    
    if (reg < 64) {
        allocated_ &= ~(1ULL << reg);
        if (reg < next_reg_) {
            next_reg_ = reg;
        }
    } else {
        large_regs_.erase(reg);
    }
}

void RegisterAllocator::release_many(const int* regs, int count) {
    for (int i = 0; i < count; i++) {
        release(regs[i]);
    }
}

void RegisterAllocator::reserve(int reg) {
    if (reg < 0) return;
    
    if (reg < 64) {
        allocated_ |= (1ULL << reg);
    } else {
        large_regs_.insert(reg);
    }
    if (reg >= max_reg_) {
        max_reg_ = reg + 1;
    }
}

void RegisterAllocator::reset() {
    allocated_ = 0;
    next_reg_ = 0;
    large_regs_.clear();
}

int RegisterAllocator::allocated_count() const {
    // Count bits set in bitmap
    int count = 0;
    uint64_t temp = allocated_;
    while (temp) {
        count += (temp & 1);
        temp >>= 1;
    }
    return count + static_cast<int>(large_regs_.size());
}

} // namespace svdb

// C-compatible API
extern "C" {

struct svdb_regalloc_t {
    svdb::RegisterAllocator impl;
};

svdb_regalloc_t* svdb_regalloc_create(int initial_regs) {
    return new svdb_regalloc_t{svdb::RegisterAllocator(initial_regs)};
}

void svdb_regalloc_destroy(svdb_regalloc_t* ra) {
    delete ra;
}

int svdb_regalloc_alloc(svdb_regalloc_t* ra) {
    return ra->impl.alloc();
}

void svdb_regalloc_alloc_many(svdb_regalloc_t* ra, int count, int* result) {
    ra->impl.alloc_many(count, result);
}

void svdb_regalloc_release(svdb_regalloc_t* ra, int reg) {
    ra->impl.release(reg);
}

void svdb_regalloc_release_many(svdb_regalloc_t* ra, const int* regs, int count) {
    ra->impl.release_many(regs, count);
}

void svdb_regalloc_reserve(svdb_regalloc_t* ra, int reg) {
    ra->impl.reserve(reg);
}

int svdb_regalloc_max_reg(const svdb_regalloc_t* ra) {
    return ra->impl.max_reg();
}

void svdb_regalloc_reset(svdb_regalloc_t* ra) {
    ra->impl.reset();
}

int svdb_regalloc_allocated_count(const svdb_regalloc_t* ra) {
    return ra->impl.allocated_count();
}

} // extern "C"
