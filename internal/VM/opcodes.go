package VM

// OpCode represents the operation codes for the VM
type OpCode int

const (
	// Control flow
	OpNull OpCode = iota
	OpGoto
	OpGosub
	OpReturn
	OpInit

	// Transaction
	OpTransaction
	OpAutoCommit

	// Cursor operations
	OpOpenRead
	OpOpenWrite
	OpOpenAutoCommit
	OpOpenWriteCursor
	OpRewind
	OpNext
	OpPrev
	OpSeek
	OpSeekGT
	OpSeekGE
	OpSeekLT
	OpFound
	OpNotFound
	OpLast
	OpClose
	OpReset

	// Result
	OpResultColumn
	OpResultRow

	// Memory operations
	OpConstNull
	OpMove
	OpCopy
	OpSCopy
	OpIntCopy
	OpLoadConst

	// Data operations
	OpColumn
	OpRowid
	OpAffinity

	// Literals
	OpString
	OpBlob
	OpInteger
	OpReal
	OpInteger16
	OpTransientBlob
	OpZeroBlob
	OpBlobZero

	// Functions
	OpFunction
	OpFunction0
	OpAggStep
	OpAggFinal
	OpCount
	OpAggregate

	// Comparison
	OpEq
	OpNe
	OpLt
	OpLe
	OpGt
	OpGe
	OpIs
	OpIsNot
	OpIsNull
	OpNotNull
	OpIfNull
	OpIfNull2

	// Arithmetic
	OpAdd
	OpSubtract
	OpMultiply
	OpDivide
	OpRemainder
	OpAddImm

	// Bitwise
	OpBitAnd
	OpBitOr
	OpShiftLeft
	OpShiftRight

	// String operations
	OpConcat
	OpSubstr
	OpLength
	OpUpper
	OpLower
	OpTrim
	OpLTrim
	OpRTrim
	OpReplace
	OpInstr
	OpLike
	OpNotLike
	OpGlob

	// Aggregate functions
	OpSum
	OpAvg
	OpMin
	OpMax
	OpTotal

	// Mathematical functions
	OpAbs
	OpRound
	OpCeil
	OpCeiling
	OpFloor
	OpPow
	OpSqrt
	OpMod
	OpExp
	OpLog
	OpLog10
	OpLn
	OpSin
	OpCos
	OpTan
	OpAsin
	OpAcos
	OpAtan
	OpAtan2
	OpSinh
	OpCosh
	OpTanh
	OpDegToRad
	OpRadToDeg

	// Type conversion
	OpToText
	OpToNumeric
	OpToInt
	OpToReal
	OpCast

	// String building
	OpMakeRecord
	OpBlobAppend

	// DML operations
	OpInsert
	OpDelete
	OpUpdate

	// Set operations
	OpUnionAll
	OpUnionDistinct
	OpExcept
	OpIntersect
	OpEphemeralCreate
	OpEphemeralInsert
	OpEphemeralFind

	// Null handling
	OpIfNot
	OpIf

	// List operations
	OpList
	OpListPush
	OpListPop
	OpListToArray
	OpArrayToList
	OpStrGroupConcat

	// Subquery
	OpScalarSubquery
	OpExistsSubquery
	OpNotExistsSubquery
	OpInSubquery
	OpNotInSubquery

	// Misc
	OpSql
	OpCursorHint
	OpNoop
	OpExplain
	OpOverlaps
	OpRealToInt
	OpHalt
	OpTypeof
	OpRandom
	OpCallScalar // Generic scalar function call: P4=*ScalarCallInfo, result in P4.Dst

	// JSON operations (future)
	OpJson
	OpJsonExtract

	// Last - must be last
	OpLastCode
)

// OpCodeInfo holds metadata about each opcode
var OpCodeInfo = map[OpCode]string{
	// Control flow
	OpNull:   "Null",
	OpGoto:   "Goto",
	OpGosub:  "Gosub",
	OpReturn: "Return",
	OpInit:   "Init",

	// Transaction
	OpTransaction: "Transaction",
	OpAutoCommit:  "AutoCommit",

	// Cursor operations
	OpOpenRead:        "OpenRead",
	OpOpenWrite:       "OpenWrite",
	OpOpenAutoCommit:  "OpenAutoCommit",
	OpOpenWriteCursor: "OpenWriteCursor",
	OpRewind:          "Rewind",
	OpNext:            "Next",
	OpPrev:            "Prev",
	OpSeek:            "Seek",
	OpSeekGT:          "SeekGT",
	OpSeekGE:          "SeekGE",
	OpSeekLT:          "SeekLT",
	OpFound:           "Found",
	OpNotFound:        "NotFound",
	OpLast:            "Last",
	OpClose:           "Close",
	OpReset:           "Reset",

	// Result
	OpResultColumn: "ResultColumn",
	OpResultRow:    "ResultRow",

	// Memory operations
	OpConstNull: "Null",
	OpMove:      "Move",
	OpCopy:      "Copy",
	OpSCopy:     "SCopy",
	OpIntCopy:   "IntCopy",
	OpLoadConst: "LoadConst",

	// Data operations
	OpColumn:   "Column",
	OpRowid:    "Rowid",
	OpAffinity: "Affinity",

	// Literals
	OpString:        "String",
	OpBlob:          "Blob",
	OpInteger:       "Integer",
	OpReal:          "Real",
	OpInteger16:     "Integer16",
	OpTransientBlob: "TransientBlob",
	OpZeroBlob:      "ZeroBlob",
	OpBlobZero:      "BlobZero",

	// Functions
	OpFunction:  "Function",
	OpFunction0: "Function0",
	OpAggStep:   "AggStep",
	OpAggFinal:  "AggFinal",
	OpCount:     "Count",
	OpAggregate: "Aggregate",

	// Comparison
	OpEq:      "Eq",
	OpNe:      "Ne",
	OpLt:      "Lt",
	OpLe:      "Le",
	OpGt:      "Gt",
	OpGe:      "Ge",
	OpIs:      "Is",
	OpIsNot:   "IsNot",
	OpIsNull:  "IsNull",
	OpNotNull: "NotNull",
	OpIfNull:  "IfNull",
	OpIfNull2: "IfNull2",

	// Arithmetic
	OpAdd:       "Add",
	OpSubtract:  "Subtract",
	OpMultiply:  "Multiply",
	OpDivide:    "Divide",
	OpRemainder: "Remainder",
	OpAddImm:    "AddImm",

	// Bitwise
	OpBitAnd:     "BitAnd",
	OpBitOr:      "BitOr",
	OpShiftLeft:  "ShiftLeft",
	OpShiftRight: "ShiftRight",

	// String operations
	OpConcat:  "Concat",
	OpSubstr:  "Substr",
	OpLength:  "Length",
	OpUpper:   "Upper",
	OpLower:   "Lower",
	OpTrim:    "Trim",
	OpLTrim:   "LTrim",
	OpRTrim:   "RTrim",
	OpReplace:  "Replace",
	OpInstr:    "Instr",
	OpLike:     "Like",
	OpNotLike:  "NotLike",
	OpGlob:     "Glob",

	// Aggregate functions
	OpSum:   "Sum",
	OpAvg:   "Avg",
	OpMin:   "Min",
	OpMax:   "Max",
	OpTotal: "Total",

	// Mathematical functions
	OpAbs:      "Abs",
	OpRound:    "Round",
	OpCeil:     "Ceil",
	OpCeiling:  "Ceiling",
	OpFloor:    "Floor",
	OpPow:      "Pow",
	OpSqrt:     "Sqrt",
	OpMod:      "Mod",
	OpExp:      "Exp",
	OpLog:      "Log",
	OpLog10:    "Log10",
	OpLn:       "Ln",
	OpSin:      "Sin",
	OpCos:      "Cos",
	OpTan:      "Tan",
	OpAsin:     "Asin",
	OpAcos:     "Acos",
	OpAtan:     "Atan",
	OpAtan2:    "Atan2",
	OpSinh:     "Sinh",
	OpCosh:     "Cosh",
	OpTanh:     "Tanh",
	OpDegToRad: "DegToRad",
	OpRadToDeg: "RadToDeg",

	// Type conversion
	OpToText:    "ToText",
	OpToNumeric: "ToNumeric",
	OpToInt:     "ToInt",
	OpToReal:    "ToReal",
	OpCast:      "Cast",

	// String building
	OpMakeRecord: "MakeRecord",
	OpBlobAppend: "BlobAppend",

	// DML operations
	OpInsert: "Insert",
	OpDelete: "Delete",
	OpUpdate: "Update",

	// Set operations
	OpUnionAll:        "UnionAll",
	OpUnionDistinct:   "UnionDistinct",
	OpExcept:          "Except",
	OpIntersect:       "Intersect",
	OpEphemeralCreate: "EphemeralCreate",
	OpEphemeralInsert: "EphemeralInsert",
	OpEphemeralFind:   "EphemeralFind",

	// Null handling
	OpIfNot: "IfNot",
	OpIf:    "If",

	// List operations
	OpList:           "List",
	OpListPush:       "ListPush",
	OpListPop:        "ListPop",
	OpListToArray:    "ListToArray",
	OpArrayToList:    "ArrayToList",
	OpStrGroupConcat: "GroupConcat",

	// Subquery
	OpScalarSubquery:    "ScalarSubquery",
	OpExistsSubquery:    "ExistsSubquery",
	OpNotExistsSubquery: "NotExistsSubquery",
	OpInSubquery:        "InSubquery",
	OpNotInSubquery:     "NotInSubquery",

	// Misc
	OpSql:        "Sql",
	OpCursorHint: "CursorHint",
	OpNoop:       "Noop",
	OpExplain:    "Explain",
	OpOverlaps:   "Overlaps",
	OpRealToInt:  "RealToInt",
	OpHalt:       "Halt",
	OpTypeof:     "Typeof",
	OpRandom:     "Random",
	OpCallScalar: "CallScalar",

	// JSON operations (future)
	OpJson:        "Json",
	OpJsonExtract: "JsonExtract",
}

// IsJump returns true if the opcode is a jump instruction
func (op OpCode) IsJump() bool {
	switch op {
	case OpGoto, OpGosub, OpIf, OpIfNot, OpEq, OpNe, OpLt, OpLe, OpGt, OpGe, OpIs, OpIsNot, OpIsNull, OpNotNull, OpFound, OpNotFound:
		return true
	}
	return false
}

// IsComparison returns true if the opcode is a comparison instruction
func (op OpCode) IsComparison() bool {
	switch op {
	case OpEq, OpNe, OpLt, OpLe, OpGt, OpGe, OpIs, OpIsNot:
		return true
	}
	return false
}
