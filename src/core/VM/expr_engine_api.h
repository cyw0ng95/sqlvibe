#ifndef SVDB_VM_EXPR_ENGINE_API_H
#define SVDB_VM_EXPR_ENGINE_API_H

/*
 * C-compatible API for ExprEngine — CGO can include this header directly.
 * The C++ class definition (in expr_engine.h) must NOT be included from CGO.
 */

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

void*   SVDB_VM_ExprEngine_Create(void);
void    SVDB_VM_ExprEngine_Destroy(void* engine);

int64_t SVDB_VM_ExprEngine_EvalIntOp(void* engine, int op, int64_t a, int64_t b);
double  SVDB_VM_ExprEngine_EvalFloatOp(void* engine, int op, double a, double b);
int     SVDB_VM_ExprEngine_EvalCompare(void* engine, int op, int64_t a, int64_t b);
int     SVDB_VM_ExprEngine_EvalLogic(void* engine, int op, int a, int b);

#ifdef __cplusplus
}
#endif

#endif /* SVDB_VM_EXPR_ENGINE_API_H */
