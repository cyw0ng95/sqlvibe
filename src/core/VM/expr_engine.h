#ifndef SVDB_VM_EXPR_ENGINE_H
#define SVDB_VM_EXPR_ENGINE_H

#include <cstdint>
#include <cstddef>
#include <string>
#include <vector>

namespace svdb {
namespace vm {

enum class ExprOp : int {
    Add = 1, Sub = 2, Mul = 3, Div = 4, Mod = 5,
    Eq = 10, Ne = 11, Lt = 12, Le = 13, Gt = 14, Ge = 15,
    And = 20, Or = 21, Not = 22
};

class ExprEngine {
public:
    ExprEngine();
    ~ExprEngine();

    int64_t EvaluateIntOp(ExprOp op, int64_t a, int64_t b);
    double EvaluateFloatOp(ExprOp op, double a, double b);
    bool EvaluateCompare(ExprOp op, int64_t a, int64_t b);
    bool EvaluateCompare(ExprOp op, double a, double b);

    bool EvaluateLogic(ExprOp op, bool a, bool b);
};

} // namespace vm
} // namespace svdb

extern "C" {

void* SVDB_VM_ExprEngine_Create();
void SVDB_VM_ExprEngine_Destroy(void* engine);

int64_t SVDB_VM_ExprEngine_EvalIntOp(void* engine, int op, int64_t a, int64_t b);
double SVDB_VM_ExprEngine_EvalFloatOp(void* engine, int op, double a, double b);
int SVDB_VM_ExprEngine_EvalCompare(void* engine, int op, int64_t a, int64_t b);
int SVDB_VM_ExprEngine_EvalLogic(void* engine, int op, int a, int b);

} // extern "C"

#endif // SVDB_VM_EXPR_ENGINE_H
