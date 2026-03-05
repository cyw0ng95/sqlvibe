#include "aggregate_engine.h"
#include <algorithm>

namespace svdb {
namespace vm {

AggregateEngine::AggregateEngine() {
}

AggregateEngine::~AggregateEngine() {
}

void AggregateEngine::Init() {
    groups_.clear();
    current_key_.clear();
}

void AggregateEngine::Reset() {
    for (auto& pair : groups_) {
        pair.second.count = 0;
        pair.second.sum_int = 0;
        pair.second.sum_float = 0.0;
        pair.second.min_val = 0.0;
        pair.second.max_val = 0.0;
        pair.second.min_set = false;
        pair.second.max_set = false;
        pair.second.group_concat.clear();
        pair.second.is_float = false;
    }
}

void AggregateEngine::SetGroupBy(const char* group_key, size_t key_len) {
    current_key_.assign(group_key, key_len);
    if (groups_.find(current_key_) == groups_.end()) {
        groups_[current_key_] = AggState();
    }
}

void AggregateEngine::ClearGroups() {
    groups_.clear();
    current_key_.clear();
}

void AggregateEngine::Accumulate(int func, int64_t int_val) {
    if (current_key_.empty()) return;
    AggState& state = groups_[current_key_];
    state.is_float = false;
    
    switch (func) {
        case static_cast<int>(AggFunc::Count):
            state.count++;
            break;
        case static_cast<int>(AggFunc::Sum):
            state.sum_int += int_val;
            state.count++;
            break;
        case static_cast<int>(AggFunc::Min):
            if (!state.min_set || int_val < state.min_val) {
                state.min_val = int_val;
                state.min_set = true;
            }
            break;
        case static_cast<int>(AggFunc::Max):
            if (!state.max_set || int_val > state.max_val) {
                state.max_val = int_val;
                state.max_set = true;
            }
            break;
    }
}

void AggregateEngine::Accumulate(int func, double float_val) {
    if (current_key_.empty()) return;
    AggState& state = groups_[current_key_];
    state.is_float = true;
    
    switch (func) {
        case static_cast<int>(AggFunc::Count):
            state.count++;
            break;
        case static_cast<int>(AggFunc::Sum):
            state.sum_float += float_val;
            state.count++;
            break;
        case static_cast<int>(AggFunc::Avg):
            state.sum_float += float_val;
            state.count++;
            break;
        case static_cast<int>(AggFunc::Min):
            if (!state.min_set || float_val < state.min_val) {
                state.min_val = float_val;
                state.min_set = true;
            }
            break;
        case static_cast<int>(AggFunc::Max):
            if (!state.max_set || float_val > state.max_val) {
                state.max_val = float_val;
                state.max_set = true;
            }
            break;
    }
}

void AggregateEngine::Accumulate(int func, const char* text_val, size_t text_len) {
    if (current_key_.empty()) return;
    AggState& state = groups_[current_key_];
    
    switch (func) {
        case static_cast<int>(AggFunc::Min):
            if (!state.min_set || std::string(text_val, text_len) < state.group_concat) {
                state.group_concat.assign(text_val, text_len);
                state.min_set = true;
            }
            break;
        case static_cast<int>(AggFunc::Max):
            if (!state.max_set || std::string(text_val, text_len) > state.group_concat) {
                state.group_concat.assign(text_val, text_len);
                state.max_set = true;
            }
            break;
        case static_cast<int>(AggFunc::GroupConcat):
            if (!state.group_concat.empty()) {
                state.group_concat += ",";
            }
            state.group_concat.append(text_val, text_len);
            break;
    }
}

int64_t AggregateEngine::GetCount(const char* group_key) const {
    auto it = groups_.find(group_key);
    return (it != groups_.end()) ? it->second.count : 0;
}

int64_t AggregateEngine::GetSumInt(const char* group_key) const {
    auto it = groups_.find(group_key);
    return (it != groups_.end()) ? it->second.sum_int : 0;
}

double AggregateEngine::GetSumFloat(const char* group_key) const {
    auto it = groups_.find(group_key);
    return (it != groups_.end()) ? it->second.sum_float : 0.0;
}

double AggregateEngine::GetAvg(const char* group_key) const {
    auto it = groups_.find(group_key);
    if (it == groups_.end() || it->second.count == 0) return 0.0;
    return it->second.sum_float / it->second.count;
}

double AggregateEngine::GetMin(const char* group_key) const {
    auto it = groups_.find(group_key);
    return (it != groups_.end() && it->second.min_set) ? it->second.min_val : 0.0;
}

double AggregateEngine::GetMax(const char* group_key) const {
    auto it = groups_.find(group_key);
    return (it != groups_.end() && it->second.max_set) ? it->second.max_val : 0.0;
}

const char* AggregateEngine::GetGroupConcat(const char* group_key) const {
    auto it = groups_.find(group_key);
    return (it != groups_.end()) ? it->second.group_concat.c_str() : "";
}

} // namespace vm
} // namespace svdb

extern "C" {

void* SVDB_VM_AggregateEngine_Create() {
    return new svdb::vm::AggregateEngine();
}

void SVDB_VM_AggregateEngine_Destroy(void* engine) {
    delete static_cast<svdb::vm::AggregateEngine*>(engine);
}

void SVDB_VM_AggregateEngine_Init(void* engine) {
    auto* e = static_cast<svdb::vm::AggregateEngine*>(engine);
    e->Init();
}

void SVDB_VM_AggregateEngine_Reset(void* engine) {
    auto* e = static_cast<svdb::vm::AggregateEngine*>(engine);
    e->Reset();
}

void SVDB_VM_AggregateEngine_SetGroupBy(void* engine, const char* key, size_t key_len) {
    auto* e = static_cast<svdb::vm::AggregateEngine*>(engine);
    e->SetGroupBy(key, key_len);
}

void SVDB_VM_AggregateEngine_AccumulateInt(void* engine, int func, int64_t val) {
    auto* e = static_cast<svdb::vm::AggregateEngine*>(engine);
    e->Accumulate(func, val);
}

void SVDB_VM_AggregateEngine_AccumulateFloat(void* engine, int func, double val) {
    auto* e = static_cast<svdb::vm::AggregateEngine*>(engine);
    e->Accumulate(func, val);
}

void SVDB_VM_AggregateEngine_AccumulateText(void* engine, int func, const char* val, size_t len) {
    auto* e = static_cast<svdb::vm::AggregateEngine*>(engine);
    e->Accumulate(func, val, len);
}

int64_t SVDB_VM_AggregateEngine_GetCount(void* engine, const char* group_key) {
    auto* e = static_cast<svdb::vm::AggregateEngine*>(engine);
    return e->GetCount(group_key);
}

int64_t SVDB_VM_AggregateEngine_GetSumInt(void* engine, const char* group_key) {
    auto* e = static_cast<svdb::vm::AggregateEngine*>(engine);
    return e->GetSumInt(group_key);
}

double SVDB_VM_AggregateEngine_GetSumFloat(void* engine, const char* group_key) {
    auto* e = static_cast<svdb::vm::AggregateEngine*>(engine);
    return e->GetSumFloat(group_key);
}

double SVDB_VM_AggregateEngine_GetAvg(void* engine, const char* group_key) {
    auto* e = static_cast<svdb::vm::AggregateEngine*>(engine);
    return e->GetAvg(group_key);
}

double SVDB_VM_AggregateEngine_GetMin(void* engine, const char* group_key) {
    auto* e = static_cast<svdb::vm::AggregateEngine*>(engine);
    return e->GetMin(group_key);
}

double SVDB_VM_AggregateEngine_GetMax(void* engine, const char* group_key) {
    auto* e = static_cast<svdb::vm::AggregateEngine*>(engine);
    return e->GetMax(group_key);
}

}
