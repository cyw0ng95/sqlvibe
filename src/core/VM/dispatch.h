#ifndef SVDB_VM_DISPATCH_H
#define SVDB_VM_DISPATCH_H

#include <cstdint>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

namespace svdb {
namespace vm {

struct VMState;
struct Instr {
    uint16_t op;
    uint16_t flags;
    int32_t p1;
    int32_t p2;
    int32_t p3;
};

class Dispatcher {
public:
    using OpcodeHandler = int(*)(VMState* vm, const Instr& instr);
    
    Dispatcher();
    ~Dispatcher();

    void RegisterHandler(uint16_t opcode, OpcodeHandler handler);
    void UnregisterHandler(uint16_t opcode);
    
    int Execute(VMState* vm, const std::vector<Instr>& program);
    int ExecuteSingle(VMState* vm, const Instr& instr);
    
    int32_t GetProgramCounter() const { return pc_; }
    void SetProgramCounter(int32_t pc) { pc_ = pc; }
    
    void SetAbortFlag() { abort_ = true; }
    void ClearAbortFlag() { abort_ = false; }
    bool IsAborted() const { return abort_; }

private:
    int32_t pc_;
    bool abort_;
    OpcodeHandler handlers_[256];
};

class VMState {
public:
    VMState();
    ~VMState();

    void SetRegister(int32_t idx, int64_t value);
    int64_t GetRegister(int32_t idx) const;
    
    void SetRegisterFloat(int32_t idx, double value);
    double GetRegisterFloat(int32_t idx) const;
    
    void SetRowCount(int64_t count) { rowCount_ = count; }
    int64_t GetRowCount() const { return rowCount_; }
    
    void SetError(const char* err);
    const char* GetError() const;
    bool HasError() const { return !error_.empty(); }
    
    void* GetUserData() const { return userData_; }
    void SetUserData(void* data) { userData_ = data; }

private:
    std::vector<int64_t> intRegisters_;
    std::vector<double> floatRegisters_;
    int64_t rowCount_;
    std::string error_;
    void* userData_;
};

} // namespace vm
} // namespace svdb

extern "C" {

void* SVDB_VM_Dispatcher_Create();
void SVDB_VM_Dispatcher_Destroy(void* disp);

void SVDB_VM_Dispatcher_Register(void* disp, uint16_t opcode, int(*handler)(void* vm, const void* instr));

int SVDB_VM_Dispatcher_Execute(void* disp, void* vm, const void* program, size_t progLen);
int SVDB_VM_Dispatcher_ExecuteSingle(void* disp, void* vm, const void* instr);

void* SVDB_VM_State_Create();
void SVDB_VM_State_Destroy(void* state);

void SVDB_VM_State_SetRegister(void* state, int32_t idx, int64_t value);
int64_t SVDB_VM_State_GetRegister(void* state, int32_t idx);

void SVDB_VM_State_SetRegisterFloat(void* state, int32_t idx, double value);
double SVDB_VM_State_GetRegisterFloat(void* state, int32_t idx);

void SVDB_VM_State_SetRowCount(void* state, int64_t count);
int64_t SVDB_VM_State_GetRowCount(void* state);

void SVDB_VM_State_SetError(void* state, const char* err);
int SVDB_VM_State_HasError(void* state);

} // extern "C"

#endif // SVDB_VM_DISPATCH_H
