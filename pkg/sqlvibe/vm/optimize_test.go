package vm_test

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
	svvm "github.com/cyw0ng95/sqlvibe/pkg/sqlvibe/vm"
)

func TestNewColumnSet(t *testing.T) {
	cs := svvm.NewColumnSet([]string{"id", "name", "age"})
	if !cs.Contains("id") || !cs.Contains("name") || !cs.Contains("age") {
		t.Error("expected all provided columns to be in the set")
	}
	if cs.Contains("missing") {
		t.Error("expected 'missing' to not be in the set")
	}
}

func TestPruneColumns_SelectStar_ReturnNil(t *testing.T) {
	stmt := &QP.SelectStmt{
		Columns: []QP.Expr{&QP.ColumnRef{Name: "*"}},
		From:    &QP.TableRef{Name: "t"},
	}
	result := svvm.PruneColumns(stmt, []string{"id", "name", "age"})
	if result != nil {
		t.Error("expected nil (no pruning) when SELECT * is used")
	}
}

func TestPruneColumns_SpecificCols(t *testing.T) {
	stmt := &QP.SelectStmt{
		Columns: []QP.Expr{
			&QP.ColumnRef{Name: "id"},
			&QP.ColumnRef{Name: "name"},
		},
		From: &QP.TableRef{Name: "t"},
	}
	available := []string{"id", "name", "age", "created_at"}
	result := svvm.PruneColumns(stmt, available)
	if result == nil {
		t.Fatal("expected pruned column list")
	}
	if len(result) != 2 {
		t.Errorf("expected 2 pruned columns, got %d: %v", len(result), result)
	}
}

func TestPruneColumns_WithWhere(t *testing.T) {
	stmt := &QP.SelectStmt{
		Columns: []QP.Expr{&QP.ColumnRef{Name: "name"}},
		From:    &QP.TableRef{Name: "t"},
		Where: &QP.BinaryExpr{
			Op:    QP.TokenEq,
			Left:  &QP.ColumnRef{Name: "id"},
			Right: &QP.Literal{Value: int64(1)},
		},
	}
	available := []string{"id", "name", "age"}
	result := svvm.PruneColumns(stmt, available)
	if result == nil {
		t.Fatal("expected pruned column list")
	}
	// id (from WHERE) + name (from SELECT) should be kept
	if len(result) != 2 {
		t.Errorf("expected 2 columns (id + name), got %d: %v", len(result), result)
	}
}

func TestPruneColumns_AllNeeded_ReturnNil(t *testing.T) {
	stmt := &QP.SelectStmt{
		Columns: []QP.Expr{
			&QP.ColumnRef{Name: "id"},
			&QP.ColumnRef{Name: "name"},
		},
		From: &QP.TableRef{Name: "t"},
	}
	available := []string{"id", "name"}
	result := svvm.PruneColumns(stmt, available)
	// All columns needed → no benefit → nil
	if result != nil {
		t.Errorf("expected nil when all columns are needed, got %v", result)
	}
}

func TestCanPushdownWhere_Nil(t *testing.T) {
	if svvm.CanPushdownWhere(nil) {
		t.Error("expected false for nil WHERE")
	}
}

func TestCanPushdownWhere_SimplePredicate(t *testing.T) {
	where := &QP.BinaryExpr{
		Op:    QP.TokenEq,
		Left:  &QP.ColumnRef{Name: "status"},
		Right: &QP.Literal{Value: "active"},
	}
	if !svvm.CanPushdownWhere(where) {
		t.Error("expected true for simple pushable predicate")
	}
}
