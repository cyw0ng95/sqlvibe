/* test_vm_execute.cpp — VM Execution Unit Tests */
#include <gtest/gtest.h>
#include "vm_execute.h"
#include <cstring>
#include <cmath>

/* Test fixture for VM execution tests */
class VMExecuteTest : public ::testing::Test {
protected:
    svdb_vm_t* vm = nullptr;
    svdb_vm_result_t result = {};

    void SetUp() override {
        vm = svdb_vm_create();
        ASSERT_NE(vm, nullptr);
    }

    void TearDown() override {
        svdb_vm_result_destroy(&result);
        svdb_vm_destroy(vm);
    }

    /* Helper to create a simple program */
    svdb_vm_program_t create_program(int32_t num_instrs, int32_t num_regs) {
        svdb_vm_program_t prog = {};
        prog.num_instructions = num_instrs;
        prog.num_registers = num_regs;
        prog.instructions = (svdb_vm_instr_t*)calloc(
            (size_t)num_instrs, sizeof(svdb_vm_instr_t));
        return prog;
    }

    /* Helper to set up an instruction */
    void setup_instr(svdb_vm_program_t* prog, int32_t idx, uint16_t opcode,
                     int32_t p1, int32_t p2, int32_t p4_int) {
        prog->instructions[idx].opcode = opcode;
        prog->instructions[idx].p1 = p1;
        prog->instructions[idx].p2 = p2;
        prog->instructions[idx].p4_type = 1;  /* int */
        prog->instructions[idx].p4_int = p4_int;
    }
};

/* Opcode constants (must match vm_execute.cpp) */
enum {
    OP_NOP = 0,
    OP_HALT = 1,
    OP_GOTO = 2,
    OP_LOAD_CONST = 10,
    OP_NULL = 11,
    OP_MOVE = 20,
    OP_COPY = 21,
    OP_ADD = 30,
    OP_SUB = 31,
    OP_MUL = 32,
    OP_DIV = 33,
    OP_EQ = 40,
    OP_NE = 41,
    OP_LT = 42,
    OP_LE = 43,
    OP_GT = 44,
    OP_GE = 45,
    OP_IF = 60,
    OP_IF_NOT = 61,
};

/* ── Basic tests ────────────────────────────────────────────────────────── */

TEST_F(VMExecuteTest, CreateDestroy) {
    /* Already tested in SetUp/TearDown */
    EXPECT_NE(vm, nullptr);
}

TEST_F(VMExecuteTest, EmptyProgram) {
    svdb_vm_program_t prog = create_program(0, 10);
    int32_t ret = svdb_vm_execute(vm, &prog, nullptr, &result);
    EXPECT_EQ(ret, 0);
    free(prog.instructions);
}

TEST_F(VMExecuteTest, HaltImmediately) {
    svdb_vm_program_t prog = create_program(1, 10);
    setup_instr(&prog, 0, OP_HALT, 0, 0, 0);
    
    int32_t ret = svdb_vm_execute(vm, &prog, nullptr, &result);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(vm->halted, 1);
    
    free(prog.instructions);
}

TEST_F(VMExecuteTest, LoadConstInt) {
    svdb_vm_program_t prog = create_program(2, 10);
    
    /* R0 = 42 */
    prog.instructions[0].opcode = OP_LOAD_CONST;
    prog.instructions[0].p1 = 0;
    prog.instructions[0].p4_type = 1;
    prog.instructions[0].p4_int = 42;
    
    /* HALT */
    prog.instructions[1].opcode = OP_HALT;
    
    int32_t ret = svdb_vm_execute(vm, &prog, nullptr, &result);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(svdb_vm_get_register_int(vm, 0), 42);
    
    free(prog.instructions);
}

TEST_F(VMExecuteTest, LoadConstFloat) {
    svdb_vm_program_t prog = create_program(2, 10);
    
    /* R0 = 3.14 */
    prog.instructions[0].opcode = OP_LOAD_CONST;
    prog.instructions[0].p1 = 0;
    prog.instructions[0].p4_type = 3;  /* float */
    prog.instructions[0].p4_float = 3.14;
    
    /* HALT */
    prog.instructions[1].opcode = OP_HALT;
    
    int32_t ret = svdb_vm_execute(vm, &prog, nullptr, &result);
    EXPECT_EQ(ret, 0);
    EXPECT_DOUBLE_EQ(svdb_vm_get_register_float(vm, 0), 3.14);
    
    free(prog.instructions);
}

TEST_F(VMExecuteTest, AddIntegers) {
    svdb_vm_program_t prog = create_program(4, 10);
    
    /* R0 = 10 */
    setup_instr(&prog, 0, OP_LOAD_CONST, 0, 0, 10);
    
    /* R1 = 20 */
    setup_instr(&prog, 1, OP_LOAD_CONST, 1, 0, 20);
    
    /* R2 = R0 + R1 */
    prog.instructions[2].opcode = OP_ADD;
    prog.instructions[2].p1 = 0;
    prog.instructions[2].p2 = 1;
    prog.instructions[2].has_dst = 1;
    prog.instructions[2].dst_reg = 2;
    
    /* HALT */
    prog.instructions[3].opcode = OP_HALT;
    
    int32_t ret = svdb_vm_execute(vm, &prog, nullptr, &result);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(svdb_vm_get_register_int(vm, 0), 10);
    EXPECT_EQ(svdb_vm_get_register_int(vm, 1), 20);
    EXPECT_EQ(svdb_vm_get_register_int(vm, 2), 30);
    
    free(prog.instructions);
}

TEST_F(VMExecuteTest, AddFloats) {
    svdb_vm_program_t prog = create_program(4, 10);
    
    /* R0 = 1.5 */
    prog.instructions[0].opcode = OP_LOAD_CONST;
    prog.instructions[0].p1 = 0;
    prog.instructions[0].p4_type = 3;
    prog.instructions[0].p4_float = 1.5;
    
    /* R1 = 2.5 */
    prog.instructions[1].opcode = OP_LOAD_CONST;
    prog.instructions[1].p1 = 1;
    prog.instructions[1].p4_type = 3;
    prog.instructions[1].p4_float = 2.5;
    
    /* R2 = R0 + R1 */
    prog.instructions[2].opcode = OP_ADD;
    prog.instructions[2].p1 = 0;
    prog.instructions[2].p2 = 1;
    prog.instructions[2].has_dst = 1;
    prog.instructions[2].dst_reg = 2;
    
    /* HALT */
    prog.instructions[3].opcode = OP_HALT;
    
    int32_t ret = svdb_vm_execute(vm, &prog, nullptr, &result);
    EXPECT_EQ(ret, 0);
    EXPECT_DOUBLE_EQ(svdb_vm_get_register_float(vm, 2), 4.0);
    
    free(prog.instructions);
}

TEST_F(VMExecuteTest, CompareEqual) {
    svdb_vm_program_t prog = create_program(4, 10);
    
    /* R0 = 5 */
    setup_instr(&prog, 0, OP_LOAD_CONST, 0, 0, 5);
    
    /* R1 = 5 */
    setup_instr(&prog, 1, OP_LOAD_CONST, 1, 0, 5);
    
    /* R2 = (R0 == R1) */
    prog.instructions[2].opcode = OP_EQ;
    prog.instructions[2].p1 = 0;
    prog.instructions[2].p2 = 1;
    prog.instructions[2].has_dst = 1;
    prog.instructions[2].dst_reg = 2;
    
    /* HALT */
    prog.instructions[3].opcode = OP_HALT;
    
    int32_t ret = svdb_vm_execute(vm, &prog, nullptr, &result);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(svdb_vm_get_register_int(vm, 2), 1);  /* true */
    
    free(prog.instructions);
}

TEST_F(VMExecuteTest, CompareNotEqual) {
    svdb_vm_program_t prog = create_program(4, 10);
    
    /* R0 = 5 */
    setup_instr(&prog, 0, OP_LOAD_CONST, 0, 0, 5);
    
    /* R1 = 10 */
    setup_instr(&prog, 1, OP_LOAD_CONST, 1, 0, 10);
    
    /* R2 = (R0 == R1) */
    prog.instructions[2].opcode = OP_EQ;
    prog.instructions[2].p1 = 0;
    prog.instructions[2].p2 = 1;
    prog.instructions[2].has_dst = 1;
    prog.instructions[2].dst_reg = 2;
    
    /* HALT */
    prog.instructions[3].opcode = OP_HALT;
    
    int32_t ret = svdb_vm_execute(vm, &prog, nullptr, &result);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(svdb_vm_get_register_int(vm, 2), 0);  /* false */
    
    free(prog.instructions);
}

TEST_F(VMExecuteTest, JumpIfTrue) {
    svdb_vm_program_t prog = create_program(4, 10);
    
    /* R0 = 1 (true) */
    setup_instr(&prog, 0, OP_LOAD_CONST, 0, 0, 1);
    
    /* IF R0 GOTO 3 */
    prog.instructions[1].opcode = OP_IF;
    prog.instructions[1].p1 = 0;
    prog.instructions[1].p2 = 3;  /* Skip instruction 2 */
    
    /* R1 = 100 (should be skipped) */
    setup_instr(&prog, 2, OP_LOAD_CONST, 1, 0, 100);
    
    /* R2 = 200 */
    setup_instr(&prog, 3, OP_LOAD_CONST, 2, 0, 200);
    
    /* HALT */
    prog.instructions[4].opcode = OP_HALT;
    prog.num_instructions = 5;
    
    int32_t ret = svdb_vm_execute(vm, &prog, nullptr, &result);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(svdb_vm_get_register_int(vm, 1), 0);  /* Not set (skipped) */
    EXPECT_EQ(svdb_vm_get_register_int(vm, 2), 200);
    
    free(prog.instructions);
}

TEST_F(VMExecuteTest, JumpIfFalse) {
    svdb_vm_program_t prog = create_program(4, 10);
    
    /* R0 = 0 (false) */
    setup_instr(&prog, 0, OP_LOAD_CONST, 0, 0, 0);
    
    /* IF NOT R0 GOTO 3 */
    prog.instructions[1].opcode = OP_IF_NOT;
    prog.instructions[1].p1 = 0;
    prog.instructions[1].p2 = 3;  /* Skip instruction 2 */
    
    /* R1 = 100 (should be skipped) */
    setup_instr(&prog, 2, OP_LOAD_CONST, 1, 0, 100);
    
    /* R2 = 200 */
    setup_instr(&prog, 3, OP_LOAD_CONST, 2, 0, 200);
    
    /* HALT */
    prog.instructions[4].opcode = OP_HALT;
    prog.num_instructions = 5;
    
    int32_t ret = svdb_vm_execute(vm, &prog, nullptr, &result);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(svdb_vm_get_register_int(vm, 1), 0);  /* Not set (skipped) */
    EXPECT_EQ(svdb_vm_get_register_int(vm, 2), 200);
    
    free(prog.instructions);
}

TEST_F(VMExecuteTest, MoveRegister) {
    svdb_vm_program_t prog = create_program(3, 10);
    
    /* R0 = 42 */
    setup_instr(&prog, 0, OP_LOAD_CONST, 0, 0, 42);
    
    /* R1 = R0 (MOVE) */
    prog.instructions[1].opcode = OP_MOVE;
    prog.instructions[1].p1 = 0;
    prog.instructions[1].p2 = 1;
    
    /* HALT */
    prog.instructions[2].opcode = OP_HALT;
    
    int32_t ret = svdb_vm_execute(vm, &prog, nullptr, &result);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(svdb_vm_get_register_int(vm, 0), 42);
    EXPECT_EQ(svdb_vm_get_register_int(vm, 1), 42);
    
    free(prog.instructions);
}

TEST_F(VMExecuteTest, MultiplyIntegers) {
    svdb_vm_program_t prog = create_program(4, 10);
    
    /* R0 = 6 */
    setup_instr(&prog, 0, OP_LOAD_CONST, 0, 0, 6);
    
    /* R1 = 7 */
    setup_instr(&prog, 1, OP_LOAD_CONST, 1, 0, 7);
    
    /* R2 = R0 * R1 */
    prog.instructions[2].opcode = OP_MUL;
    prog.instructions[2].p1 = 0;
    prog.instructions[2].p2 = 1;
    prog.instructions[2].has_dst = 1;
    prog.instructions[2].dst_reg = 2;
    
    /* HALT */
    prog.instructions[3].opcode = OP_HALT;
    
    int32_t ret = svdb_vm_execute(vm, &prog, nullptr, &result);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(svdb_vm_get_register_int(vm, 2), 42);
    
    free(prog.instructions);
}

TEST_F(VMExecuteTest, DivideFloats) {
    svdb_vm_program_t prog = create_program(4, 10);
    
    /* R0 = 10.0 */
    prog.instructions[0].opcode = OP_LOAD_CONST;
    prog.instructions[0].p1 = 0;
    prog.instructions[0].p4_type = 3;
    prog.instructions[0].p4_float = 10.0;
    
    /* R1 = 2.0 */
    prog.instructions[1].opcode = OP_LOAD_CONST;
    prog.instructions[1].p1 = 1;
    prog.instructions[1].p4_type = 3;
    prog.instructions[1].p4_float = 2.0;
    
    /* R2 = R0 / R1 */
    prog.instructions[2].opcode = OP_DIV;
    prog.instructions[2].p1 = 0;
    prog.instructions[2].p2 = 1;
    prog.instructions[2].has_dst = 1;
    prog.instructions[2].dst_reg = 2;
    
    /* HALT */
    prog.instructions[3].opcode = OP_HALT;
    
    int32_t ret = svdb_vm_execute(vm, &prog, nullptr, &result);
    EXPECT_EQ(ret, 0);
    EXPECT_DOUBLE_EQ(svdb_vm_get_register_float(vm, 2), 5.0);
    
    free(prog.instructions);
}

TEST_F(VMExecuteTest, LessThan) {
    svdb_vm_program_t prog = create_program(4, 10);
    
    /* R0 = 5 */
    setup_instr(&prog, 0, OP_LOAD_CONST, 0, 0, 5);
    
    /* R1 = 10 */
    setup_instr(&prog, 1, OP_LOAD_CONST, 1, 0, 10);
    
    /* R2 = (R0 < R1) */
    prog.instructions[2].opcode = OP_LT;
    prog.instructions[2].p1 = 0;
    prog.instructions[2].p2 = 1;
    prog.instructions[2].has_dst = 1;
    prog.instructions[2].dst_reg = 2;
    
    /* HALT */
    prog.instructions[3].opcode = OP_HALT;
    
    int32_t ret = svdb_vm_execute(vm, &prog, nullptr, &result);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(svdb_vm_get_register_int(vm, 2), 1);  /* true */
    
    free(prog.instructions);
}

TEST_F(VMExecuteTest, GreaterThanOrEqual) {
    svdb_vm_program_t prog = create_program(4, 10);
    
    /* R0 = 10 */
    setup_instr(&prog, 0, OP_LOAD_CONST, 0, 0, 10);
    
    /* R1 = 10 */
    setup_instr(&prog, 1, OP_LOAD_CONST, 1, 0, 10);
    
    /* R2 = (R0 >= R1) */
    prog.instructions[2].opcode = OP_GE;
    prog.instructions[2].p1 = 0;
    prog.instructions[2].p2 = 1;
    prog.instructions[2].has_dst = 1;
    prog.instructions[2].dst_reg = 2;
    
    /* HALT */
    prog.instructions[3].opcode = OP_HALT;
    
    int32_t ret = svdb_vm_execute(vm, &prog, nullptr, &result);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(svdb_vm_get_register_int(vm, 2), 1);  /* true (equal) */
    
    free(prog.instructions);
}

TEST_F(VMExecuteTest, Sqrt) {
    svdb_vm_program_t prog = create_program(3, 10);
    
    /* R0 = 16.0 */
    prog.instructions[0].opcode = OP_LOAD_CONST;
    prog.instructions[0].p1 = 0;
    prog.instructions[0].p4_type = 3;
    prog.instructions[0].p4_float = 16.0;
    
    /* R1 = sqrt(R0) */
    prog.instructions[1].opcode = 114;  /* OP_SQRT */
    prog.instructions[1].p1 = 0;
    prog.instructions[1].has_dst = 1;
    prog.instructions[1].dst_reg = 1;
    
    /* HALT */
    prog.instructions[2].opcode = OP_HALT;
    
    int32_t ret = svdb_vm_execute(vm, &prog, nullptr, &result);
    EXPECT_EQ(ret, 0);
    EXPECT_DOUBLE_EQ(svdb_vm_get_register_float(vm, 1), 4.0);
    
    free(prog.instructions);
}

TEST_F(VMExecuteTest, Typeof) {
    svdb_vm_program_t prog = create_program(4, 10);
    
    /* R0 = 42 (integer) */
    setup_instr(&prog, 0, OP_LOAD_CONST, 0, 0, 42);
    
    /* R1 = typeof(R0) */
    prog.instructions[1].opcode = 130;  /* OP_TYPEOF */
    prog.instructions[1].p1 = 0;
    prog.instructions[1].has_dst = 1;
    prog.instructions[1].dst_reg = 1;
    
    /* HALT */
    prog.instructions[2].opcode = OP_HALT;
    
    int32_t ret = svdb_vm_execute(vm, &prog, nullptr, &result);
    EXPECT_EQ(ret, 0);
    EXPECT_STREQ(svdb_vm_get_register_text(vm, 1), "integer");
    
    free(prog.instructions);
}

/* ── Main ───────────────────────────────────────────────────────────────── */

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
