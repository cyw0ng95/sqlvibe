package QP

import (
	"strings"
	"testing"
)

// TestCParser_Select tests basic SELECT parsing.
func TestCParser_Select(t *testing.T) {
	node, errMsg := ParseSQL("SELECT id, name FROM users WHERE id = 1")
	if node == nil {
		t.Fatalf("ParseSQL returned nil: %s", errMsg)
	}
	if node.Type() != ASTSelect {
		t.Errorf("expected ASTSelect (%d), got %d", ASTSelect, node.Type())
	}
	if node.Table() != "users" {
		t.Errorf("expected table 'users', got %q", node.Table())
	}
	cols := node.Columns()
	if len(cols) < 2 {
		t.Errorf("expected >= 2 columns, got %d: %v", len(cols), cols)
	}
	if node.Where() == "" {
		t.Error("expected non-empty WHERE clause")
	}
}

// TestCParser_SelectStar tests SELECT * parsing.
func TestCParser_SelectStar(t *testing.T) {
	node, errMsg := ParseSQL("SELECT * FROM orders")
	if node == nil {
		t.Fatalf("ParseSQL returned nil: %s", errMsg)
	}
	if node.Type() != ASTSelect {
		t.Errorf("expected ASTSelect, got %d", node.Type())
	}
	if node.Table() != "orders" {
		t.Errorf("expected table 'orders', got %q", node.Table())
	}
	cols := node.Columns()
	if len(cols) != 1 || cols[0] != "*" {
		t.Errorf("expected [*], got %v", cols)
	}
}

// TestCParser_Insert tests INSERT INTO parsing.
func TestCParser_Insert(t *testing.T) {
	node, errMsg := ParseSQL("INSERT INTO products (id, name, price) VALUES (1, 'Widget', 9.99)")
	if node == nil {
		t.Fatalf("ParseSQL returned nil: %s", errMsg)
	}
	if node.Type() != ASTInsert {
		t.Errorf("expected ASTInsert (%d), got %d", ASTInsert, node.Type())
	}
	if node.Table() != "products" {
		t.Errorf("expected table 'products', got %q", node.Table())
	}
	cols := node.Columns()
	if len(cols) != 3 {
		t.Errorf("expected 3 columns, got %d: %v", len(cols), cols)
	}
	if node.ValueRowCount() != 1 {
		t.Errorf("expected 1 value row, got %d", node.ValueRowCount())
	}
	if node.ValueCount(0) != 3 {
		t.Errorf("expected 3 values in row 0, got %d", node.ValueCount(0))
	}
}

// TestCParser_InsertMultiRow tests INSERT with multiple value rows.
func TestCParser_InsertMultiRow(t *testing.T) {
	node, errMsg := ParseSQL("INSERT INTO t (a, b) VALUES (1, 2), (3, 4), (5, 6)")
	if node == nil {
		t.Fatalf("ParseSQL returned nil: %s", errMsg)
	}
	if node.ValueRowCount() != 3 {
		t.Errorf("expected 3 value rows, got %d", node.ValueRowCount())
	}
}

// TestCParser_Delete tests DELETE FROM parsing.
func TestCParser_Delete(t *testing.T) {
	node, errMsg := ParseSQL("DELETE FROM customers WHERE id = 42")
	if node == nil {
		t.Fatalf("ParseSQL returned nil: %s", errMsg)
	}
	if node.Type() != ASTDelete {
		t.Errorf("expected ASTDelete (%d), got %d", ASTDelete, node.Type())
	}
	if node.Table() != "customers" {
		t.Errorf("expected table 'customers', got %q", node.Table())
	}
	if node.Where() == "" {
		t.Error("expected non-empty WHERE clause")
	}
}

// TestCParser_CreateTable tests CREATE TABLE parsing.
func TestCParser_CreateTable(t *testing.T) {
	node, errMsg := ParseSQL("CREATE TABLE employees (id INTEGER, name TEXT, salary REAL)")
	if node == nil {
		t.Fatalf("ParseSQL returned nil: %s", errMsg)
	}
	if node.Type() != ASTCreate {
		t.Errorf("expected ASTCreate (%d), got %d", ASTCreate, node.Type())
	}
	if node.Table() != "employees" {
		t.Errorf("expected table 'employees', got %q", node.Table())
	}
	if node.ColumnCount() == 0 {
		t.Error("expected columns in CREATE TABLE")
	}
}

// TestCParser_DropTable tests DROP TABLE parsing.
func TestCParser_DropTable(t *testing.T) {
	node, errMsg := ParseSQL("DROP TABLE IF EXISTS tmp_data")
	if node == nil {
		t.Fatalf("ParseSQL returned nil: %s", errMsg)
	}
	if node.Type() != ASTDrop {
		t.Errorf("expected ASTDrop (%d), got %d", ASTDrop, node.Type())
	}
	if node.Table() != "tmp_data" {
		t.Errorf("expected table 'tmp_data', got %q", node.Table())
	}
}

// TestCParser_SQLText tests that the original SQL text is preserved.
func TestCParser_SQLText(t *testing.T) {
	sql := "SELECT id FROM users"
	node, errMsg := ParseSQL(sql)
	if node == nil {
		t.Fatalf("ParseSQL returned nil: %s", errMsg)
	}
	if got := node.SQL(); !strings.Contains(got, "SELECT") {
		t.Errorf("expected SQL text to contain 'SELECT', got %q", got)
	}
}

// TestCParser_UnknownSQL tests that unknown SQL returns nil with error.
func TestCParser_UnknownSQL(t *testing.T) {
	node, errMsg := ParseSQL("EXPLAIN SELECT 1")
	if node != nil {
		t.Error("expected nil node for unsupported statement")
	}
	if errMsg == "" {
		t.Error("expected non-empty error for unsupported statement")
	}
}

// TestCParser_Update tests UPDATE parsing with parallel columns/values.
func TestCParser_Update(t *testing.T) {
	node, errMsg := ParseSQL("UPDATE orders SET status = 'shipped', qty = 5 WHERE id = 10")
	if node == nil {
		t.Fatalf("ParseSQL returned nil: %s", errMsg)
	}
	if node.Type() != ASTUpdate {
		t.Errorf("expected ASTUpdate (%d), got %d", ASTUpdate, node.Type())
	}
	if node.Table() != "orders" {
		t.Errorf("expected table 'orders', got %q", node.Table())
	}
	cols := node.Columns()
	if len(cols) != 2 {
		t.Errorf("expected 2 SET columns, got %d: %v", len(cols), cols)
	}
	// All values stored in a single parallel row: values[0][i] ↔ columns[i]
	if node.ValueRowCount() != 1 {
		t.Errorf("expected 1 value row for UPDATE, got %d", node.ValueRowCount())
	}
	if node.ValueCount(0) != 2 {
		t.Errorf("expected 2 values in row 0, got %d", node.ValueCount(0))
	}
	if node.Where() == "" {
		t.Error("expected non-empty WHERE clause")
	}
}

// TestCParser_EmptySQL tests that empty SQL is handled gracefully.
func TestCParser_EmptySQL(t *testing.T) {
	node, _ := ParseSQL("")
	// May return nil for empty SQL
	if node != nil && node.Type() != ASTUnknown {
		t.Errorf("unexpected node type for empty SQL: %d", node.Type())
	}
}
