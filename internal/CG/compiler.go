package CG

import (
	"fmt"

	QP "github.com/sqlvibe/sqlvibe/internal/QP"
	VM "github.com/sqlvibe/sqlvibe/internal/VM"
)

// Compiler wraps the VM compiler and provides a clean API for code generation.
// This is a transitional structure - the compiler logic will be fully moved
// from VM to CG in future commits.
type Compiler struct {
	vmCompiler *VM.Compiler
}

func NewCompiler() *Compiler {
	return &Compiler{
		vmCompiler: VM.NewCompiler(),
	}
}

func (c *Compiler) CompileSelect(stmt *QP.SelectStmt) *VM.Program {
	return c.vmCompiler.CompileSelect(stmt)
}

func (c *Compiler) CompileInsert(stmt *QP.InsertStmt) *VM.Program {
	return c.vmCompiler.CompileInsert(stmt)
}

func (c *Compiler) CompileUpdate(stmt *QP.UpdateStmt) *VM.Program {
	return c.vmCompiler.CompileUpdate(stmt)
}

func (c *Compiler) CompileDelete(stmt *QP.DeleteStmt) *VM.Program {
	return c.vmCompiler.CompileDelete(stmt)
}

func (c *Compiler) CompileAggregate(stmt *QP.SelectStmt) *VM.Program {
	return c.vmCompiler.CompileAggregate(stmt)
}

func (c *Compiler) Program() *VM.Program {
	return c.vmCompiler.Program()
}

// SetTableSchema sets the table schema for SELECT * expansion
func (c *Compiler) SetTableSchema(schema map[string]int, schemaOrder []string) {
	c.vmCompiler.TableColIndices = schema
	c.vmCompiler.TableColOrder = schemaOrder
}

// SetMultiTableSchema sets multi-table schema for JOIN queries
func (c *Compiler) SetMultiTableSchema(schemas map[string]map[string]int, colOrder []string) {
	c.vmCompiler.TableSchemas = schemas
	c.vmCompiler.TableColOrder = colOrder
}

// GetVMCompiler returns the underlying VM compiler (for internal use)
func (c *Compiler) GetVMCompiler() *VM.Compiler {
	return c.vmCompiler
}

// Compile compiles SQL string into bytecode program
func Compile(sql string) (*VM.Program, error) {
	return CompileWithSchema(sql, nil)
}

// CompileWithSchema compiles SQL with table schema information
func CompileWithSchema(sql string, tableColumns []string) (*VM.Program, error) {
	tokenizer := QP.NewTokenizer(sql)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, err
	}

	parser := QP.NewParser(tokens)
	stmt, err := parser.Parse()
	if err != nil {
		return nil, err
	}

	c := NewCompiler()
	c.SetTableSchema(make(map[string]int), tableColumns)
	for i, col := range tableColumns {
		c.vmCompiler.TableColIndices[col] = i
	}

	switch s := stmt.(type) {
	case *QP.SelectStmt:
		if hasAggregates(s) {
			return c.CompileAggregate(s), nil
		}
		return c.CompileSelect(s), nil
	case *QP.InsertStmt:
		return c.CompileInsert(s), nil
	case *QP.UpdateStmt:
		return c.CompileUpdate(s), nil
	case *QP.DeleteStmt:
		return c.CompileDelete(s), nil
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

func hasAggregates(stmt *QP.SelectStmt) bool {
	if stmt == nil {
		return false
	}
	for _, col := range stmt.Columns {
		if fc, ok := col.(*QP.FuncCall); ok {
			switch fc.Name {
			case "COUNT", "SUM", "AVG", "MIN", "MAX", "TOTAL":
				return true
			}
		}
	}
	return stmt.GroupBy != nil
}

func MustCompile(sql string) *VM.Program {
	prog, err := Compile(sql)
	if err != nil {
		panic(err)
	}
	return prog
}
