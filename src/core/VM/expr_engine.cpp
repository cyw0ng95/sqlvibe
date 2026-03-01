#include "expr_engine.h"

namespace svdb {
namespace vm {

ExprEngine::ExprEngine() {
}

ExprEngine::~ExprEngine() {
}

int64_t ExprEngine::EvaluateIntOp(ExprOp op, int64_t a, int64_t b) {
    switch (op) {
        case ExprOp::Add: return a + b;
        case ExprOp::Sub: return a - b;
        case ExprOp::Mul: return a * b;
        case ExprOp::Div: return b != 0 ? a / b : 0;
        case ExprOp::Mod: return b != 0 ? a % b : 0;
        default: return 0;
    }
}

double ExprEngine::EvaluateFloatOp(ExprOp op, double a, double b) {
    switch (op) {
        case ExprOp::Add: return a + b;
        case ExprOp::Sub: return a - b;
        case ExprOp::Mul: return a * b;
        case ExprOp::Div: return b != 0.0 ? a / b : 0.0;
        default: return 0.0;
    }
}

bool ExprEngine::EvaluateCompare(ExprOp op, int64_t a, int64_t b) {
    switch (op) {
        case ExprOp::Eq: return a == b;
        case ExprOp::Ne: return a != b;
        case ExprOp::Lt: return a < b;
        case ExprOp::Le: return a <= b;
        case ExprOp::Gt: return a > b;
        case ExprOp::Ge: return a >= b;
        default: return false;
    }
}

bool ExprEngine::EvaluateCompare(ExprOp op, double a, double b) {
    switch (op) {
        case ExprOp::Eq: return a == b;
        case ExprOp::Ne: return a != b;
        case ExprOp::Lt: return a < b;
        case ExprOp::Le: return a <= b;
        case ExprOp::Gt: return a > b;
        case ExprOp::Ge: return a >= b;
        default: return false;
    }
}

bool ExprEngine::EvaluateLogic(ExprOp op, bool a, bool b) {
    switch (op) {
        case ExprOp::And: return a && b;
        case ExprOp::Or:  return a || b;
        default: return false;
    }
}

} // namespace vm
} // namespace svdb

extern "C" {

void* SVDB_VM_ExprEngine_Create() {
    return new svdb::vm::ExprEngine();
}

void SVDB_VM_ExprEngine_Destroy(void* engine) {
    delete static_cast<svdb::vm::ExprEngine*>(engine);
}

int64_t SVDB_VM_ExprEngine_EvalIntOp(void* engine, int op, int64_t a, int64_t b) {
    auto* e = static_cast<svdb::vm::ExprEngine*>(engine);
    return e->EvaluateIntOp(static_cast<svdb::vm::ExprOp>(op), a, b);
}

double SVDB_VM_ExprEngine_EvalFloatOp(void* engine, int op, double a, double b) {
    auto* e = static_cast<svdb::vm::ExprEngine*>(engine);
    return e->EvaluateFloatOp(static_cast<svdb::vm::ExprOp>(op), a, b);
}

int SVDB_VM_ExprEngine_EvalCompare(void* engine, int op, int64_t a, int64_t b) {
    auto* e = static_cast<svdb::vm::ExprEngine*>(engine);
    return e->EvaluateCompare(static_cast<svdb::vm::ExprOp>(op), a, b) ? 1 : 0;
}

int SVDB_VM_ExprEngine_EvalLogic(void* engine, int op, int a, int b) {
    auto* e = static_cast<svdb::vm::ExprEngine*>(engine);
    return e->EvaluateLogic(static_cast<svdb::vm::ExprOp>(op), a != 0, b != 0) ? 1 : 0;
}

}
