#ifndef SVDB_VM_REGISTERS_H
#define SVDB_VM_REGISTERS_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
#include <unordered_set>

namespace svdb {

// RegisterAllocator manages VM register allocation.
// Uses a bitmap for registers 0-63 (fast path) and a set for larger registers.
class RegisterAllocator {
public:
    RegisterAllocator(int initial_regs = 16);
    ~RegisterAllocator() = default;
    
    // Allocate a single register, returns register number
    int alloc();
    
    // Allocate multiple consecutive registers
    void alloc_many(int count, int* result);
    
    // Release a single register
    void release(int reg);
    
    // Release multiple registers
    void release_many(const int* regs, int count);
    
    // Reserve a specific register
    void reserve(int reg);
    
    // Get the maximum register number + 1
    int max_reg() const { return max_reg_; }
    
    // Reset the allocator
    void reset();
    
    // Get the number of allocated registers
    int allocated_count() const;

private:
    int max_reg_;
    int next_reg_;
    uint64_t allocated_;  // Bitmap for registers 0-63
    std::unordered_set<int> large_regs_;  // Registers >= 64
};

} // namespace svdb
#endif // __cplusplus

#ifdef __cplusplus
extern "C" {
#endif

// Opaque handle for register allocator
typedef struct svdb_regalloc_t svdb_regalloc_t;

// Create a new register allocator
svdb_regalloc_t* svdb_regalloc_create(int initial_regs);

// Destroy a register allocator
void svdb_regalloc_destroy(svdb_regalloc_t* ra);

// Allocate a single register
int svdb_regalloc_alloc(svdb_regalloc_t* ra);

// Allocate multiple registers (caller must provide result array)
void svdb_regalloc_alloc_many(svdb_regalloc_t* ra, int count, int* result);

// Release a single register
void svdb_regalloc_release(svdb_regalloc_t* ra, int reg);

// Release multiple registers
void svdb_regalloc_release_many(svdb_regalloc_t* ra, const int* regs, int count);

// Reserve a specific register
void svdb_regalloc_reserve(svdb_regalloc_t* ra, int reg);

// Get maximum register number + 1
int svdb_regalloc_max_reg(const svdb_regalloc_t* ra);

// Reset the allocator
void svdb_regalloc_reset(svdb_regalloc_t* ra);

// Get allocated count
int svdb_regalloc_allocated_count(const svdb_regalloc_t* ra);

#ifdef __cplusplus
}
#endif

#endif // SVDB_VM_REGISTERS_H
