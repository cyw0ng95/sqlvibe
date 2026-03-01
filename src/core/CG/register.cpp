#include "register.h"
#include <algorithm>
#include <cstring>

namespace svdb {
namespace cg {

RegisterAllocator::RegisterAllocator(int32_t baseReg)
    : baseReg_(baseReg), nextReg_(baseReg), maxReg_(baseReg - 1) {
}

RegisterAllocator::~RegisterAllocator() {
}

int32_t RegisterAllocator::Alloc() {
    if (!freeList_.empty()) {
        int32_t reg = freeList_.top();
        freeList_.pop();
        activeRegs_.insert(reg);
        return reg;
    }
    int32_t reg = nextReg_++;
    activeRegs_.insert(reg);
    if (reg > maxReg_) maxReg_ = reg;
    return reg;
}

void RegisterAllocator::Free(int32_t reg) {
    if (reg >= baseReg_) {
        activeRegs_.erase(reg);
        freeList_.push(reg);
    }
}

void RegisterAllocator::Reset() {
    while (!freeList_.empty()) freeList_.pop();
    nextReg_ = baseReg_;
    maxReg_ = baseReg_ - 1;
    activeRegs_.clear();
    useCount_.clear();
}

int32_t RegisterAllocator::GetUseCount(int32_t reg) const {
    if (reg >= 0 && reg < static_cast<int32_t>(useCount_.size())) {
        return useCount_[reg];
    }
    return 0;
}

void RegisterAllocator::MarkUsed(int32_t reg) {
    if (reg >= 0) {
        if (reg >= static_cast<int32_t>(useCount_.size())) {
            useCount_.resize(reg + 1, 0);
        }
        useCount_[reg]++;
    }
}

InstrEmitter::InstrEmitter() {
}

InstrEmitter::~InstrEmitter() {
}

void InstrEmitter::Emit(uint16_t op, int32_t p1, int32_t p2, int32_t p3) {
    Instr instr;
    instr.op = op;
    instr.flags = 0;
    instr.p1 = p1;
    instr.p2 = p2;
    instr.p3 = p3;
    instructions_.push_back(instr);
}

void InstrEmitter::EmitWithFlags(uint16_t op, uint16_t flags, int32_t p1, int32_t p2, int32_t p3) {
    Instr instr;
    instr.op = op;
    instr.flags = flags;
    instr.p1 = p1;
    instr.p2 = p2;
    instr.p3 = p3;
    instructions_.push_back(instr);
}

int32_t InstrEmitter::EmitGoto(int32_t target) {
    int32_t pos = GetPosition();
    Emit(0, target, 0, 0);  // opcode 0 = Goto placeholder
    pendingFixups_.push_back(instructions_.size() - 1);
    return pos;
}

void InstrEmitter::Fixup(int32_t fixupPos, int32_t target) {
    if (fixupPos >= 0 && fixupPos < static_cast<int32_t>(instructions_.size())) {
        instructions_[fixupPos].p1 = target;
    }
}

void InstrEmitter::Clear() {
    instructions_.clear();
    pendingFixups_.clear();
}

ExprCompiler::ExprCompiler(RegisterAllocator* ra, InstrEmitter* emitter)
    : ra_(ra), emitter_(emitter) {
}

ExprCompiler::~ExprCompiler() {
}

int32_t ExprCompiler::Compile(int32_t exprType, const void* exprData) {
    return ra_->Alloc();
}

void ExprCompiler::SetColumnIndex(const char* colName, int32_t index) {
    columnIndex_[colName] = index;
}

void ExprCompiler::SetTableColumnIndex(const char* tableName, const char* colName, int32_t index) {
    tableColumnIndex_[tableName][colName] = index;
}

} // namespace cg
} // namespace svdb

// C-compatible wrapper functions
extern "C" {

void* SVDB_CG_RegisterAllocator_Create(int32_t baseReg) {
    return new svdb::cg::RegisterAllocator(baseReg);
}

void SVDB_CG_RegisterAllocator_Destroy(void* ra) {
    delete static_cast<svdb::cg::RegisterAllocator*>(ra);
}

int32_t SVDB_CG_RegisterAllocator_Alloc(void* ra) {
    auto* r = static_cast<svdb::cg::RegisterAllocator*>(ra);
    return r->Alloc();
}

void SVDB_CG_RegisterAllocator_Free(void* ra, int32_t reg) {
    auto* r = static_cast<svdb::cg::RegisterAllocator*>(ra);
    r->Free(reg);
}

void SVDB_CG_RegisterAllocator_Reset(void* ra) {
    auto* r = static_cast<svdb::cg::RegisterAllocator*>(ra);
    r->Reset();
}

void* SVDB_CG_InstrEmitter_Create() {
    return new svdb::cg::InstrEmitter();
}

void SVDB_CG_InstrEmitter_Destroy(void* emitter) {
    delete static_cast<svdb::cg::InstrEmitter*>(emitter);
}

void SVDB_CG_InstrEmitter_Emit(void* emitter, uint16_t op, int32_t p1, int32_t p2, int32_t p3) {
    auto* e = static_cast<svdb::cg::InstrEmitter*>(emitter);
    e->Emit(op, p1, p2, p3);
}

int32_t SVDB_CG_InstrEmitter_GetPosition(void* emitter) {
    auto* e = static_cast<svdb::cg::InstrEmitter*>(emitter);
    return e->GetPosition();
}

void SVDB_CG_InstrEmitter_Fixup(void* emitter, int32_t pos, int32_t target) {
    auto* e = static_cast<svdb::cg::InstrEmitter*>(emitter);
    e->Fixup(pos, target);
}

size_t SVDB_CG_InstrEmitter_GetCount(void* emitter) {
    auto* e = static_cast<svdb::cg::InstrEmitter*>(emitter);
    return e->GetInstructions().size();
}

void SVDB_CG_InstrEmitter_GetData(void* emitter, void* outBuf) {
    auto* e = static_cast<svdb::cg::InstrEmitter*>(emitter);
    const auto& instrs = e->GetInstructions();
    auto* out = static_cast<uint8_t*>(outBuf);
    std::memcpy(out, instrs.data(), instrs.size() * sizeof(svdb::cg::InstrEmitter::Instr));
}

}
