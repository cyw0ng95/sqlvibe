#ifndef SVDB_CG_REGISTER_H
#define SVDB_CG_REGISTER_H

#include <cstdint>
#include <cstddef>
#include <vector>
#include <stack>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace svdb {
namespace cg {

class RegisterAllocator {
public:
    RegisterAllocator(int32_t baseReg = 0);
    ~RegisterAllocator();

    int32_t Alloc();
    void Free(int32_t reg);
    void Reset();
    
    int32_t GetMaxReg() const { return maxReg_; }
    int32_t GetUseCount(int32_t reg) const;
    void MarkUsed(int32_t reg);
    
private:
    int32_t baseReg_;
    int32_t nextReg_;
    int32_t maxReg_;
    std::stack<int32_t> freeList_;
    std::vector<int32_t> useCount_;
    std::unordered_set<int32_t> activeRegs_;
};

class InstrEmitter {
public:
    InstrEmitter();
    ~InstrEmitter();

    struct Instr {
        uint16_t op;
        uint16_t flags;
        int32_t p1;
        int32_t p2;
        int32_t p3;
    };

    void Emit(uint16_t op, int32_t p1 = 0, int32_t p2 = 0, int32_t p3 = 0);
    void EmitWithFlags(uint16_t op, uint16_t flags, int32_t p1 = 0, int32_t p2 = 0, int32_t p3 = 0);
    
    int32_t EmitGoto(int32_t target);
    void Fixup(int32_t fixupPos, int32_t target);
    
    const std::vector<Instr>& GetInstructions() const { return instructions_; }
    std::vector<Instr>& GetInstructions() { return instructions_; }
    void Clear();
    
    int32_t GetPosition() const { return static_cast<int32_t>(instructions_.size()); }

private:
    std::vector<Instr> instructions_;
    std::vector<size_t> pendingFixups_;
};

class ExprCompiler {
public:
    ExprCompiler(RegisterAllocator* ra, InstrEmitter* emitter);
    ~ExprCompiler();

    int32_t Compile(int32_t exprType, const void* exprData);
    
    void SetColumnIndex(const char* colName, int32_t index);
    void SetTableColumnIndex(const char* tableName, const char* colName, int32_t index);

private:
    RegisterAllocator* ra_;
    InstrEmitter* emitter_;
    std::unordered_map<std::string, int32_t> columnIndex_;
    std::unordered_map<std::string, std::unordered_map<std::string, int32_t>> tableColumnIndex_;
};

} // namespace cg
} // namespace svdb

extern "C" {

void* SVDB_CG_RegisterAllocator_Create(int32_t baseReg);
void SVDB_CG_RegisterAllocator_Destroy(void* ra);

int32_t SVDB_CG_RegisterAllocator_Alloc(void* ra);
void SVDB_CG_RegisterAllocator_Free(void* ra, int32_t reg);
void SVDB_CG_RegisterAllocator_Reset(void* ra);

void* SVDB_CG_InstrEmitter_Create();
void SVDB_CG_InstrEmitter_Destroy(void* emitter);

void SVDB_CG_InstrEmitter_Emit(void* emitter, uint16_t op, int32_t p1, int32_t p2, int32_t p3);
int32_t SVDB_CG_InstrEmitter_GetPosition(void* emitter);
void SVDB_CG_InstrEmitter_Fixup(void* emitter, int32_t pos, int32_t target);

size_t SVDB_CG_InstrEmitter_GetCount(void* emitter);
void SVDB_CG_InstrEmitter_GetData(void* emitter, void* outBuf);

} // extern "C"

#endif // SVDB_CG_REGISTER_H
