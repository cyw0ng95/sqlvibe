#include "dispatch.h"
#include <cstring>

namespace svdb {
namespace vm {

Dispatcher::Dispatcher() : pc_(0), abort_(false) {
    memset(handlers_, 0, sizeof(handlers_));
}

Dispatcher::~Dispatcher() {
}

void Dispatcher::RegisterHandler(uint16_t opcode, OpcodeHandler handler) {
    if (opcode < 256) {
        handlers_[opcode] = handler;
    }
}

void Dispatcher::UnregisterHandler(uint16_t opcode) {
    if (opcode < 256) {
        handlers_[opcode] = nullptr;
    }
}

int Dispatcher::Execute(VMState* vm, const std::vector<Instr>& program) {
    pc_ = 0;
    abort_ = false;
    
    while (pc_ < static_cast<int32_t>(program.size()) && !abort_) {
        const Instr& instr = program[pc_];
        if (instr.op < 256 && handlers_[instr.op] != nullptr) {
            int result = handlers_[instr.op](vm, instr);
            if (result != 0) {
                return result;
            }
        }
        pc_++;
    }
    
    return 0;
}

int Dispatcher::ExecuteSingle(VMState* vm, const Instr& instr) {
    if (instr.op < 256 && handlers_[instr.op] != nullptr) {
        return handlers_[instr.op](vm, instr);
    }
    return -1;
}

VMState::VMState() : rowCount_(0), userData_(nullptr) {
}

VMState::~VMState() {
}

void VMState::SetRegister(int32_t idx, int64_t value) {
    if (idx >= 0) {
        if (idx >= static_cast<int32_t>(intRegisters_.size())) {
            intRegisters_.resize(idx + 1, 0);
        }
        intRegisters_[idx] = value;
    }
}

int64_t VMState::GetRegister(int32_t idx) const {
    if (idx >= 0 && idx < static_cast<int32_t>(intRegisters_.size())) {
        return intRegisters_[idx];
    }
    return 0;
}

void VMState::SetRegisterFloat(int32_t idx, double value) {
    if (idx >= 0) {
        if (idx >= static_cast<int32_t>(floatRegisters_.size())) {
            floatRegisters_.resize(idx + 1, 0.0);
        }
        floatRegisters_[idx] = value;
    }
}

double VMState::GetRegisterFloat(int32_t idx) const {
    if (idx >= 0 && idx < static_cast<int32_t>(floatRegisters_.size())) {
        return floatRegisters_[idx];
    }
    return 0.0;
}

void VMState::SetError(const char* err) {
    error_ = err;
}

const char* VMState::GetError() const {
    return error_.c_str();
}

} // namespace vm
} // namespace svdb

// C-compatible wrapper functions
extern "C" {

void* SVDB_VM_Dispatcher_Create() {
    return new svdb::vm::Dispatcher();
}

void SVDB_VM_Dispatcher_Destroy(void* disp) {
    delete static_cast<svdb::vm::Dispatcher*>(disp);
}

void SVDB_VM_Dispatcher_Register(void* disp, uint16_t opcode, int(*handler)(void* vm, const void* instr)) {
    auto* d = static_cast<svdb::vm::Dispatcher*>(disp);
    // Cast to the function pointer type
    d->RegisterHandler(opcode, reinterpret_cast<svdb::vm::Dispatcher::OpcodeHandler>(handler));
}

int SVDB_VM_Dispatcher_Execute(void* disp, void* vm, const void* program, size_t progLen) {
    auto* d = static_cast<svdb::vm::Dispatcher*>(disp);
    auto* state = static_cast<svdb::vm::VMState*>(vm);
    auto* prog = static_cast<const svdb::vm::Instr*>(program);
    std::vector<svdb::vm::Instr> progVec(prog, prog + progLen);
    return d->Execute(state, progVec);
}

int SVDB_VM_Dispatcher_ExecuteSingle(void* disp, void* vm, const void* instr) {
    auto* d = static_cast<svdb::vm::Dispatcher*>(disp);
    auto* state = static_cast<svdb::vm::VMState*>(vm);
    auto* i = static_cast<const svdb::vm::Instr*>(instr);
    return d->ExecuteSingle(state, *i);
}

void* SVDB_VM_State_Create() {
    return new svdb::vm::VMState();
}

void SVDB_VM_State_Destroy(void* state) {
    delete static_cast<svdb::vm::VMState*>(state);
}

void SVDB_VM_State_SetRegister(void* state, int32_t idx, int64_t value) {
    auto* s = static_cast<svdb::vm::VMState*>(state);
    s->SetRegister(idx, value);
}

int64_t SVDB_VM_State_GetRegister(void* state, int32_t idx) {
    auto* s = static_cast<svdb::vm::VMState*>(state);
    return s->GetRegister(idx);
}

void SVDB_VM_State_SetRegisterFloat(void* state, int32_t idx, double value) {
    auto* s = static_cast<svdb::vm::VMState*>(state);
    s->SetRegisterFloat(idx, value);
}

double SVDB_VM_State_GetRegisterFloat(void* state, int32_t idx) {
    auto* s = static_cast<svdb::vm::VMState*>(state);
    return s->GetRegisterFloat(idx);
}

void SVDB_VM_State_SetRowCount(void* state, int64_t count) {
    auto* s = static_cast<svdb::vm::VMState*>(state);
    s->SetRowCount(count);
}

int64_t SVDB_VM_State_GetRowCount(void* state) {
    auto* s = static_cast<svdb::vm::VMState*>(state);
    return s->GetRowCount();
}

void SVDB_VM_State_SetError(void* state, const char* err) {
    auto* s = static_cast<svdb::vm::VMState*>(state);
    s->SetError(err);
}

int SVDB_VM_State_HasError(void* state) {
    auto* s = static_cast<svdb::vm::VMState*>(state);
    return s->HasError() ? 1 : 0;
}

}
