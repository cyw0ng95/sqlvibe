package vm_test

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
	svvm "github.com/cyw0ng95/sqlvibe/pkg/sqlvibe/vm"
)

func TestExtractLimitInt_NoLimit(t *testing.T) {
	n := svvm.ExtractLimitInt(nil, nil)
	if n != 0 {
		t.Errorf("expected 0 for nil limit, got %d", n)
	}
}

func TestExtractLimitInt_LimitOnly(t *testing.T) {
	lit := &QP.Literal{Value: int64(10)}
	n := svvm.ExtractLimitInt(lit, nil)
	if n != 10 {
		t.Errorf("expected 10, got %d", n)
	}
}

func TestExtractLimitInt_LimitWithOffset(t *testing.T) {
	lim := &QP.Literal{Value: int64(5)}
	off := &QP.Literal{Value: int64(3)}
	n := svvm.ExtractLimitInt(lim, off)
	if n != 8 {
		t.Errorf("expected 8 (5+3), got %d", n)
	}
}

func TestExtractLimitInt_ZeroLimit(t *testing.T) {
	lit := &QP.Literal{Value: int64(0)}
	n := svvm.ExtractLimitInt(lit, nil)
	if n != 0 {
		t.Errorf("expected 0 for LIMIT 0, got %d", n)
	}
}

func TestIsSimpleSelectStar_True(t *testing.T) {
	stmt := &QP.SelectStmt{
		Columns: []QP.Expr{&QP.ColumnRef{Name: "*"}},
		From:    &QP.TableRef{Name: "t"},
	}
	if !svvm.IsSimpleSelectStar(stmt) {
		t.Error("expected true for simple SELECT *")
	}
}

func TestIsSimpleSelectStar_WithWhere(t *testing.T) {
	stmt := &QP.SelectStmt{
		Columns: []QP.Expr{&QP.ColumnRef{Name: "*"}},
		From:    &QP.TableRef{Name: "t"},
		Where:   &QP.ColumnRef{Name: "id"},
	}
	if svvm.IsSimpleSelectStar(stmt) {
		t.Error("expected false when WHERE is present")
	}
}

func TestIsSimpleSelectStar_NamedColumn(t *testing.T) {
	stmt := &QP.SelectStmt{
		Columns: []QP.Expr{&QP.ColumnRef{Name: "id"}},
		From:    &QP.TableRef{Name: "t"},
	}
	if svvm.IsSimpleSelectStar(stmt) {
		t.Error("expected false for non-star SELECT")
	}
}

func TestIsSimpleSelectStar_Nil(t *testing.T) {
	if svvm.IsSimpleSelectStar(nil) {
		t.Error("expected false for nil stmt")
	}
}

func TestDeduplicateRows_RemovesDuplicates(t *testing.T) {
	rows := [][]interface{}{
		{int64(1), "alice"},
		{int64(2), "bob"},
		{int64(1), "alice"},
	}
	result := svvm.DeduplicateRows(rows)
	if len(result) != 2 {
		t.Errorf("expected 2 unique rows, got %d", len(result))
	}
}

func TestDeduplicateRows_Empty(t *testing.T) {
	result := svvm.DeduplicateRows(nil)
	if len(result) != 0 {
		t.Errorf("expected empty result, got %d rows", len(result))
	}
}

func TestDeduplicateRowsN_OnlyFirstN(t *testing.T) {
	// Rows differ only in the 3rd column; with n=2 they should be deduped.
	rows := [][]interface{}{
		{int64(1), "a", "x"},
		{int64(1), "a", "y"},
		{int64(2), "b", "x"},
	}
	result := svvm.DeduplicateRowsN(rows, 2)
	if len(result) != 2 {
		t.Errorf("expected 2 rows (first 2 cols as key), got %d", len(result))
	}
}

func TestDeduplicateRowsN_AllUnique(t *testing.T) {
	rows := [][]interface{}{
		{int64(1), "a"},
		{int64(2), "b"},
	}
	result := svvm.DeduplicateRowsN(rows, 2)
	if len(result) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result))
	}
}
