#ifndef SVDB_VM_AGGREGATE_ENGINE_H
#define SVDB_VM_AGGREGATE_ENGINE_H

#include <cstdint>
#include <cstddef>
#include <string>
#include <vector>
#include <unordered_map>

namespace svdb {
namespace vm {

enum class AggFunc : int {
    Count = 1,
    Sum = 2,
    Avg = 3,
    Min = 4,
    Max = 5,
    GroupConcat = 6
};

struct AggState {
    int64_t count;
    int64_t sum_int;
    double sum_float;
    double min_val;
    double max_val;
    bool min_set;
    bool max_set;
    std::string group_concat;
    bool is_float;
};

class AggregateEngine {
public:
    AggregateEngine();
    ~AggregateEngine();

    void Init();
    void Reset();

    void SetGroupBy(const char* group_key, size_t key_len);
    void ClearGroups();

    void Accumulate(int func, int64_t int_val);
    void Accumulate(int func, double float_val);
    void Accumulate(int func, const char* text_val, size_t text_len);

    int GetGroupCount() const { return groups_.size(); }
    int64_t GetCount(const char* group_key) const;
    int64_t GetSumInt(const char* group_key) const;
    double GetSumFloat(const char* group_key) const;
    double GetAvg(const char* group_key) const;
    double GetMin(const char* group_key) const;
    double GetMax(const char* group_key) const;
    const char* GetGroupConcat(const char* group_key) const;

private:
    std::unordered_map<std::string, AggState> groups_;
    std::string current_key_;
};

} // namespace vm
} // namespace svdb

extern "C" {

void* SVDB_VM_AggregateEngine_Create();
void SVDB_VM_AggregateEngine_Destroy(void* engine);

void SVDB_VM_AggregateEngine_Init(void* engine);
void SVDB_VM_AggregateEngine_Reset(void* engine);

void SVDB_VM_AggregateEngine_SetGroupBy(void* engine, const char* key, size_t key_len);

void SVDB_VM_AggregateEngine_AccumulateInt(void* engine, int func, int64_t val);
void SVDB_VM_AggregateEngine_AccumulateFloat(void* engine, int func, double val);
void SVDB_VM_AggregateEngine_AccumulateText(void* engine, int func, const char* val, size_t len);

int64_t SVDB_VM_AggregateEngine_GetCount(void* engine, const char* group_key);
int64_t SVDB_VM_AggregateEngine_GetSumInt(void* engine, const char* group_key);
double SVDB_VM_AggregateEngine_GetSumFloat(void* engine, const char* group_key);
double SVDB_VM_AggregateEngine_GetAvg(void* engine, const char* group_key);
double SVDB_VM_AggregateEngine_GetMin(void* engine, const char* group_key);
double SVDB_VM_AggregateEngine_GetMax(void* engine, const char* group_key);

} // extern "C"

#endif // SVDB_VM_AGGREGATE_ENGINE_H
