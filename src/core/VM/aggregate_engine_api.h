#ifndef SVDB_VM_AGGREGATE_ENGINE_API_H
#define SVDB_VM_AGGREGATE_ENGINE_API_H

/*
 * C-compatible API for AggregateEngine — CGO can include this header directly.
 */

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

void*   SVDB_VM_AggregateEngine_Create(void);
void    SVDB_VM_AggregateEngine_Destroy(void* engine);

void    SVDB_VM_AggregateEngine_Init(void* engine);
void    SVDB_VM_AggregateEngine_Reset(void* engine);

void    SVDB_VM_AggregateEngine_SetGroupBy(void* engine, const char* key, size_t key_len);

void    SVDB_VM_AggregateEngine_AccumulateInt(void* engine, int func, int64_t val);
void    SVDB_VM_AggregateEngine_AccumulateFloat(void* engine, int func, double val);
void    SVDB_VM_AggregateEngine_AccumulateText(void* engine, int func, const char* val, size_t len);

int64_t SVDB_VM_AggregateEngine_GetCount(void* engine, const char* group_key);
int64_t SVDB_VM_AggregateEngine_GetSumInt(void* engine, const char* group_key);
double  SVDB_VM_AggregateEngine_GetSumFloat(void* engine, const char* group_key);
double  SVDB_VM_AggregateEngine_GetAvg(void* engine, const char* group_key);
double  SVDB_VM_AggregateEngine_GetMin(void* engine, const char* group_key);
double  SVDB_VM_AggregateEngine_GetMax(void* engine, const char* group_key);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_VM_AGGREGATE_ENGINE_API_H */
