package CG

import (
	"testing"

	QP "github.com/cyw0ng95/sqlvibe/internal/QP"
	VM "github.com/cyw0ng95/sqlvibe/internal/VM"
)

func TestCompiler_CompileInsert(t *testing.T) {
	c := NewCompiler()

	stmt := &QP.InsertStmt{
		Table:   "users",
		Columns: []string{"id", "name"},
		Values: [][]QP.Expr{
			{
				&QP.Literal{Value: int64(1)},
				&QP.Literal{Value: "Alice"},
			},
		},
	}

	prog := c.CompileInsert(stmt)
	if prog == nil {
		t.Error("CompileInsert should not return nil")
	}
	if len(prog.Instructions) == 0 {
		t.Error("Program should have instructions")
	}

	// Check for OpInsert instruction
	foundInsert := false
	for _, inst := range prog.Instructions {
		if inst.Op == VM.OpInsert {
			foundInsert = true
			break
		}
	}
	if !foundInsert {
		t.Error("Program should contain OpInsert instruction")
	}
}

func TestCompiler_CompileInsert_Defaults(t *testing.T) {
	c := NewCompiler()

	stmt := &QP.InsertStmt{
		Table:       "users",
		UseDefaults: true,
	}

	prog := c.CompileInsert(stmt)
	if prog == nil {
		t.Error("CompileInsert should not return nil")
	}

	// Check for OpInsert instruction
	foundInsert := false
	for _, inst := range prog.Instructions {
		if inst.Op == VM.OpInsert {
			foundInsert = true
			break
		}
	}
	if !foundInsert {
		t.Error("Program should contain OpInsert instruction for defaults")
	}
}

func TestCompiler_CompileInsert_MultipleRows(t *testing.T) {
	c := NewCompiler()

	stmt := &QP.InsertStmt{
		Table:   "users",
		Columns: []string{"id", "name"},
		Values: [][]QP.Expr{
			{
				&QP.Literal{Value: int64(1)},
				&QP.Literal{Value: "Alice"},
			},
			{
				&QP.Literal{Value: int64(2)},
				&QP.Literal{Value: "Bob"},
			},
		},
	}

	prog := c.CompileInsert(stmt)
	if prog == nil {
		t.Error("CompileInsert should not return nil")
	}

	// Should have two OpInsert instructions
	insertCount := 0
	for _, inst := range prog.Instructions {
		if inst.Op == VM.OpInsert {
			insertCount++
		}
	}
	if insertCount != 2 {
		t.Errorf("Expected 2 OpInsert instructions, got %d", insertCount)
	}
}

func TestCompiler_CompileUpdate(t *testing.T) {
	c := NewCompiler()

	stmt := &QP.UpdateStmt{
		Table: "users",
		Set: []QP.SetClause{
			{
				Column: &QP.ColumnRef{Name: "name"},
				Value:  &QP.Literal{Value: "Updated"},
			},
		},
	}

	prog := c.CompileUpdate(stmt)
	if prog == nil {
		t.Error("CompileUpdate should not return nil")
	}
	if len(prog.Instructions) == 0 {
		t.Error("Program should have instructions")
	}

	// Check for OpUpdate instruction
	foundUpdate := false
	for _, inst := range prog.Instructions {
		if inst.Op == VM.OpUpdate {
			foundUpdate = true
			break
		}
	}
	if !foundUpdate {
		t.Error("Program should contain OpUpdate instruction")
	}
}

func TestCompiler_CompileUpdate_WithWhere(t *testing.T) {
	c := NewCompiler()

	stmt := &QP.UpdateStmt{
		Table: "users",
		Set: []QP.SetClause{
			{
				Column: &QP.ColumnRef{Name: "name"},
				Value:  &QP.Literal{Value: "Updated"},
			},
		},
		Where: &QP.BinaryExpr{
			Op:    QP.TokenEq,
			Left:  &QP.ColumnRef{Name: "id"},
			Right: &QP.Literal{Value: int64(1)},
		},
	}

	prog := c.CompileUpdate(stmt)
	if prog == nil {
		t.Error("CompileUpdate should not return nil")
	}

	// Check for OpIfNot instruction (for WHERE clause)
	foundIfNot := false
	for _, inst := range prog.Instructions {
		if inst.Op == VM.OpIfNot {
			foundIfNot = true
			break
		}
	}
	if !foundIfNot {
		t.Error("Program should contain OpIfNot instruction for WHERE clause")
	}
}

func TestCompiler_CompileUpdate_MultipleColumns(t *testing.T) {
	c := NewCompiler()

	stmt := &QP.UpdateStmt{
		Table: "users",
		Set: []QP.SetClause{
			{
				Column: &QP.ColumnRef{Name: "name"},
				Value:  &QP.Literal{Value: "NewName"},
			},
			{
				Column: &QP.ColumnRef{Name: "age"},
				Value:  &QP.Literal{Value: int64(30)},
			},
		},
	}

	prog := c.CompileUpdate(stmt)
	if prog == nil {
		t.Error("CompileUpdate should not return nil")
	}

	// Check for OpUpdate instruction with multiple columns
	for _, inst := range prog.Instructions {
		if inst.Op == VM.OpUpdate {
			setInfo, ok := inst.P4.(map[string]int)
			if !ok {
				t.Error("OpUpdate P4 should be map[string]int")
			}
			if len(setInfo) != 2 {
				t.Errorf("Expected 2 columns in update, got %d", len(setInfo))
			}
			break
		}
	}
}

func TestCompiler_CompileDelete(t *testing.T) {
	c := NewCompiler()

	stmt := &QP.DeleteStmt{
		Table: "users",
	}

	prog := c.CompileDelete(stmt)
	if prog == nil {
		t.Error("CompileDelete should not return nil")
	}
	if len(prog.Instructions) == 0 {
		t.Error("Program should have instructions")
	}

	// Check for OpDelete instruction
	foundDelete := false
	for _, inst := range prog.Instructions {
		if inst.Op == VM.OpDelete {
			foundDelete = true
			break
		}
	}
	if !foundDelete {
		t.Error("Program should contain OpDelete instruction")
	}
}

func TestCompiler_CompileDelete_WithWhere(t *testing.T) {
	c := NewCompiler()

	stmt := &QP.DeleteStmt{
		Table: "users",
		Where: &QP.BinaryExpr{
			Op:    QP.TokenEq,
			Left:  &QP.ColumnRef{Name: "id"},
			Right: &QP.Literal{Value: int64(1)},
		},
	}

	prog := c.CompileDelete(stmt)
	if prog == nil {
		t.Error("CompileDelete should not return nil")
	}

	// Check for OpIfNot instruction (for WHERE clause)
	foundIfNot := false
	for _, inst := range prog.Instructions {
		if inst.Op == VM.OpIfNot {
			foundIfNot = true
			break
		}
	}
	if !foundIfNot {
		t.Error("Program should contain OpIfNot instruction for WHERE clause")
	}
}

func TestCompiler_CompileSelect_WithFrom(t *testing.T) {
	c := NewCompiler()
	c.TableColIndices = map[string]int{"id": 0, "name": 1}

	stmt := &QP.SelectStmt{
		Columns: []QP.Expr{
			&QP.ColumnRef{Name: "id"},
			&QP.ColumnRef{Name: "name"},
		},
		From: &QP.TableRef{Name: "users"},
	}

	prog := c.CompileSelect(stmt)
	if prog == nil {
		t.Error("CompileSelect should not return nil")
	}
	if len(prog.Instructions) == 0 {
		t.Error("Program should have instructions")
	}
}

func TestCompiler_CompileSelect_WithWhere(t *testing.T) {
	c := NewCompiler()
	c.TableColIndices = map[string]int{"id": 0, "name": 1}

	stmt := &QP.SelectStmt{
		Columns: []QP.Expr{
			&QP.ColumnRef{Name: "id"},
		},
		From: &QP.TableRef{Name: "users"},
		Where: &QP.BinaryExpr{
			Op:    QP.TokenGt,
			Left:  &QP.ColumnRef{Name: "id"},
			Right: &QP.Literal{Value: int64(10)},
		},
	}

	prog := c.CompileSelect(stmt)
	if prog == nil {
		t.Error("CompileSelect should not return nil")
	}
	if len(prog.Instructions) == 0 {
		t.Error("Program should have instructions")
	}

	// Verify program contains comparison operations
	foundComparison := false
	for _, inst := range prog.Instructions {
		if inst.Op == VM.OpGt {
			foundComparison = true
			break
		}
	}
	if !foundComparison {
		t.Error("Program should contain OpGt instruction for WHERE clause")
	}
}

func TestCompiler_CompileAggregate_Count(t *testing.T) {
	c := NewCompiler()

	stmt := &QP.SelectStmt{
		Columns: []QP.Expr{
			&QP.FuncCall{Name: "COUNT", Args: []QP.Expr{&QP.Literal{Value: int64(1)}}},
		},
		From: &QP.TableRef{Name: "users"},
	}

	prog := c.CompileAggregate(stmt)
	if prog == nil {
		t.Error("CompileAggregate should not return nil")
	}
	if len(prog.Instructions) == 0 {
		t.Error("Program should have instructions")
	}

	// Check for OpAggregate instruction
	foundAggregate := false
	for _, inst := range prog.Instructions {
		if inst.Op == VM.OpAggregate {
			foundAggregate = true
			break
		}
	}
	if !foundAggregate {
		t.Error("Program should contain OpAggregate instruction")
	}
}

func TestCompiler_CompileAggregate_WithGroupBy(t *testing.T) {
	c := NewCompiler()

	stmt := &QP.SelectStmt{
		Columns: []QP.Expr{
			&QP.ColumnRef{Name: "dept"},
			&QP.FuncCall{Name: "COUNT", Args: []QP.Expr{&QP.Literal{Value: int64(1)}}},
		},
		From: &QP.TableRef{Name: "users"},
		GroupBy: []QP.Expr{
			&QP.ColumnRef{Name: "dept"},
		},
	}

	prog := c.CompileAggregate(stmt)
	if prog == nil {
		t.Error("CompileAggregate should not return nil")
	}

	// Find OpAggregate and verify GroupByExprs
	for _, inst := range prog.Instructions {
		if inst.Op == VM.OpAggregate {
			aggInfo, ok := inst.P4.(*VM.AggregateInfo)
			if !ok {
				t.Error("P4 should be *AggregateInfo")
			}
			if len(aggInfo.GroupByExprs) != 1 {
				t.Errorf("Expected 1 group by expression, got %d", len(aggInfo.GroupByExprs))
			}
			break
		}
	}
}

func TestCompiler_CompileAggregate_SumAvgMinMax(t *testing.T) {
	c := NewCompiler()

	tests := []struct {
		name string
		stmt *QP.SelectStmt
	}{
		{
			name: "SUM",
			stmt: &QP.SelectStmt{
				Columns: []QP.Expr{
					&QP.FuncCall{Name: "SUM", Args: []QP.Expr{&QP.ColumnRef{Name: "salary"}}},
				},
				From: &QP.TableRef{Name: "employees"},
			},
		},
		{
			name: "AVG",
			stmt: &QP.SelectStmt{
				Columns: []QP.Expr{
					&QP.FuncCall{Name: "AVG", Args: []QP.Expr{&QP.ColumnRef{Name: "age"}}},
				},
				From: &QP.TableRef{Name: "users"},
			},
		},
		{
			name: "MIN",
			stmt: &QP.SelectStmt{
				Columns: []QP.Expr{
					&QP.FuncCall{Name: "MIN", Args: []QP.Expr{&QP.ColumnRef{Name: "price"}}},
				},
				From: &QP.TableRef{Name: "products"},
			},
		},
		{
			name: "MAX",
			stmt: &QP.SelectStmt{
				Columns: []QP.Expr{
					&QP.FuncCall{Name: "MAX", Args: []QP.Expr{&QP.ColumnRef{Name: "price"}}},
				},
				From: &QP.TableRef{Name: "products"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prog := c.CompileAggregate(tt.stmt)
			if prog == nil {
				t.Errorf("CompileAggregate for %s should not return nil", tt.name)
			}
		})
	}
}

func TestCompiler_CompileAggregate_Having(t *testing.T) {
	c := NewCompiler()

	stmt := &QP.SelectStmt{
		Columns: []QP.Expr{
			&QP.ColumnRef{Name: "dept"},
			&QP.FuncCall{Name: "COUNT", Args: []QP.Expr{&QP.Literal{Value: int64(1)}}},
		},
		From: &QP.TableRef{Name: "users"},
		GroupBy: []QP.Expr{
			&QP.ColumnRef{Name: "dept"},
		},
		Having: &QP.BinaryExpr{
			Op:    QP.TokenGt,
			Left:  &QP.FuncCall{Name: "COUNT", Args: []QP.Expr{&QP.Literal{Value: int64(1)}}},
			Right: &QP.Literal{Value: int64(5)},
		},
	}

	prog := c.CompileAggregate(stmt)
	if prog == nil {
		t.Error("CompileAggregate should not return nil")
	}

	// Verify HavingExpr is stored
	for _, inst := range prog.Instructions {
		if inst.Op == VM.OpAggregate {
			aggInfo, ok := inst.P4.(*VM.AggregateInfo)
			if !ok {
				t.Error("P4 should be *AggregateInfo")
			}
			if aggInfo.HavingExpr == nil {
				t.Error("HavingExpr should be set")
			}
			break
		}
	}
}

func TestCompiler_SetMultiTableSchema(t *testing.T) {
	c := NewCompiler()

	schemas := map[string]map[string]int{
		"users":    {"id": 0, "name": 1},
		"orders":   {"id": 0, "user_id": 1, "amount": 2},
		"products": {"id": 0, "price": 1},
	}
	colOrder := []string{"id", "name", "user_id", "amount", "price"}

	c.SetMultiTableSchema(schemas, colOrder)

	if c.TableSchemas == nil {
		t.Error("TableSchemas should be set")
	}
	if len(c.TableSchemas) != 3 {
		t.Errorf("Expected 3 table schemas, got %d", len(c.TableSchemas))
	}
}

func TestCompiler_ResolveColumnCount(t *testing.T) {
	c := NewCompiler()

	// Test with regular columns
	columns := []QP.Expr{
		&QP.ColumnRef{Name: "id"},
		&QP.ColumnRef{Name: "name"},
	}
	count := c.resolveColumnCount(columns)
	if count != 2 {
		t.Errorf("Expected 2 columns, got %d", count)
	}

	// Test with star column
	c.TableColOrder = []string{"id", "name", "email"}
	starColumns := []QP.Expr{
		&QP.ColumnRef{Name: "*"},
	}
	count = c.resolveColumnCount(starColumns)
	if count != 3 {
		t.Errorf("Expected 3 columns for star, got %d", count)
	}
}

func TestHasAggregates_NilStmt(t *testing.T) {
	if HasAggregates(nil) {
		t.Error("HasAggregates(nil) should return false")
	}
}

func TestHasAggregates_WithGroupBy(t *testing.T) {
	stmt := &QP.SelectStmt{
		Columns: []QP.Expr{
			&QP.ColumnRef{Name: "dept"},
		},
		GroupBy: []QP.Expr{
			&QP.ColumnRef{Name: "dept"},
		},
	}

	if !HasAggregates(stmt) {
		t.Error("HasAggregates should return true for GROUP BY")
	}
}