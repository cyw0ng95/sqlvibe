package IS

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/DS"
	"github.com/cyw0ng95/sqlvibe/internal/PB"
)

// --- SchemaExtractor ----------------------------------------------------------

func TestSchemaExtractor_ExtractTables(t *testing.T) {
	se := NewSchemaExtractor(&DS.BTree{})
	tables, err := se.ExtractTables()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tables) != 0 {
		t.Errorf("expected 0 tables, got %d", len(tables))
	}
}

func TestSchemaExtractor_ExtractColumns(t *testing.T) {
	se := NewSchemaExtractor(&DS.BTree{})
	cols, err := se.ExtractColumns("users")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cols) != 0 {
		t.Errorf("expected 0 columns, got %d", len(cols))
	}
}

func TestSchemaExtractor_GetAllColumns(t *testing.T) {
	se := NewSchemaExtractor(&DS.BTree{})
	cols, err := se.GetAllColumns()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cols) != 0 {
		t.Errorf("expected 0 columns, got %d", len(cols))
	}
}

func TestSchemaExtractor_ExtractViews(t *testing.T) {
	se := NewSchemaExtractor(&DS.BTree{})
	views, err := se.ExtractViews()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(views) != 0 {
		t.Errorf("expected 0 views, got %d", len(views))
	}
}

func TestSchemaExtractor_ExtractConstraints(t *testing.T) {
	se := NewSchemaExtractor(&DS.BTree{})
	c, err := se.ExtractConstraints("users")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(c) != 0 {
		t.Errorf("expected 0 constraints, got %d", len(c))
	}
}

func TestSchemaExtractor_GetAllConstraints(t *testing.T) {
	se := NewSchemaExtractor(&DS.BTree{})
	c, err := se.GetAllConstraints()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(c) != 0 {
		t.Errorf("expected 0 constraints, got %d", len(c))
	}
}

func TestSchemaExtractor_GetReferentialConstraints(t *testing.T) {
	se := NewSchemaExtractor(&DS.BTree{})
	refs, err := se.GetReferentialConstraints()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(refs) != 0 {
		t.Errorf("expected 0 refs, got %d", len(refs))
	}
}

// --- MetadataProvider --------------------------------------------------------

func TestMetadataProvider_GetTables(t *testing.T) {
	mp := NewMetadataProvider(&DS.BTree{})
	tables, err := mp.GetTables()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = tables
}

func TestMetadataProvider_GetColumns(t *testing.T) {
	mp := NewMetadataProvider(&DS.BTree{})
	cols, err := mp.GetColumns("orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = cols
}

func TestMetadataProvider_GetAllColumns(t *testing.T) {
	mp := NewMetadataProvider(&DS.BTree{})
	cols, err := mp.GetAllColumns()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = cols
}

func TestMetadataProvider_GetViews(t *testing.T) {
	mp := NewMetadataProvider(&DS.BTree{})
	views, err := mp.GetViews()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = views
}

func TestMetadataProvider_GetConstraints(t *testing.T) {
	mp := NewMetadataProvider(&DS.BTree{})
	c, err := mp.GetConstraints("orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = c
}

func TestMetadataProvider_GetAllConstraints(t *testing.T) {
	mp := NewMetadataProvider(&DS.BTree{})
	c, err := mp.GetAllConstraints()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = c
}

func TestMetadataProvider_GetReferentialConstraints(t *testing.T) {
	mp := NewMetadataProvider(&DS.BTree{})
	refs, err := mp.GetReferentialConstraints()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = refs
}

// --- COLUMNSView -------------------------------------------------------------

func TestCOLUMNSView_Query(t *testing.T) {
	cv := NewCOLUMNSView(&DS.BTree{})

	// All columns
	cols, err := cv.Query("", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = cols

	// Specific table
	cols, err = cv.Query("", "users")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = cols

	// Wrong schema returns nil
	cols, err = cv.Query("other_schema", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cols != nil {
		t.Errorf("expected nil for unknown schema, got %v", cols)
	}
}

func TestCOLUMNSView_ToSQL(t *testing.T) {
	ci := ColumnInfo{
		ColumnName:    "id",
		TableName:     "users",
		TableSchema:   "main",
		DataType:      "INTEGER",
		IsNullable:    "NO",
		ColumnDefault: "",
	}
	row := ci.ToSQL()
	if len(row) != 6 {
		t.Errorf("expected 6 columns, got %d", len(row))
	}
}

func TestCOLUMNSView_Columns(t *testing.T) {
	cv := NewCOLUMNSView(&DS.BTree{})
	rows, err := cv.Columns(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Returns nil (TODO placeholder)
	_ = rows
}

// --- TABLESView --------------------------------------------------------------

func TestTABLESView_Query(t *testing.T) {
	tv := NewTABLESView(&DS.BTree{})

	// All tables
	tables, err := tv.Query("", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = tables

	// Specific table (not in empty extractor)
	tables, err = tv.Query("", "users")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tables) != 0 {
		t.Errorf("expected empty, got %v", tables)
	}

	// Wrong schema
	tables, err = tv.Query("other", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tables != nil {
		t.Errorf("expected nil for unknown schema")
	}

	// main schema — just the schema filter branch
	tables, err = tv.Query("main", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = tables
}

func TestTABLESView_ToSQL(t *testing.T) {
	ti := TableInfo{
		TableName:   "users",
		TableSchema: "main",
		TableType:   ViewTypeBaseTable,
	}
	row := ti.ToSQL()
	if len(row) != 3 {
		t.Errorf("expected 3 columns, got %d", len(row))
	}
}

func TestTABLESView_Table(t *testing.T) {
	tv := NewTABLESView(&DS.BTree{})
	rows, err := tv.Table(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = rows
}

// --- VIEWSView ---------------------------------------------------------------

func TestVIEWSView_Query(t *testing.T) {
	vv := NewVIEWSView(&DS.BTree{})

	// All views
	views, err := vv.Query("", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = views

	// Specific view (not present)
	views, err = vv.Query("", "myview")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if views != nil {
		t.Errorf("expected nil")
	}

	// Wrong schema
	views, err = vv.Query("other", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if views != nil {
		t.Errorf("expected nil for unknown schema")
	}
}

func TestVIEWSView_ToSQL(t *testing.T) {
	vi := ViewInfo{
		TableName:      "myview",
		TableSchema:    "main",
		ViewDefinition: "SELECT 1",
	}
	row := vi.ToSQL()
	if len(row) != 3 {
		t.Errorf("expected 3 columns, got %d", len(row))
	}
}

func TestVIEWSView_View(t *testing.T) {
	vv := NewVIEWSView(&DS.BTree{})
	rows, err := vv.View(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = rows
}

// --- CONSTRAINTSView ---------------------------------------------------------

func TestCONSTRAINTSView_Query(t *testing.T) {
	cv := NewCONSTRAINTSView(&DS.BTree{})

	// All constraints
	c, err := cv.Query("", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = c

	// Specific table
	c, err = cv.Query("", "users")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = c

	// Wrong schema
	c, err = cv.Query("other", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c != nil {
		t.Errorf("expected nil for unknown schema")
	}

	// Main schema filter branch (no constraints in empty extractor)
	c, err = cv.Query("main", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = c
}

func TestCONSTRAINTSView_ToSQL(t *testing.T) {
	ci := ConstraintInfo{
		ConstraintName: "pk_users",
		TableName:      "users",
		TableSchema:    "main",
		ConstraintType: ConstraintTypePrimaryKey,
	}
	row := ci.ToSQL()
	if len(row) != 4 {
		t.Errorf("expected 4 columns, got %d", len(row))
	}
}

func TestCONSTRAINTSView_Constraints(t *testing.T) {
	cv := NewCONSTRAINTSView(&DS.BTree{})
	rows, err := cv.Constraints(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = rows
}

// --- REFERENTIALView ---------------------------------------------------------

func TestREFERENTIALView_Query(t *testing.T) {
	rv := NewREFERENTIALView(&DS.BTree{})

	// All refs
	refs, err := rv.Query("", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = refs

	// Filter by name (not present)
	refs, err = rv.Query("", "fk_orders_users")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(refs) != 0 {
		t.Errorf("expected 0 refs, got %d", len(refs))
	}

	// Wrong schema
	refs, err = rv.Query("other", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if refs != nil {
		t.Errorf("expected nil for unknown schema")
	}
}

func TestREFERENTIALView_ToSQL(t *testing.T) {
	rc := ReferentialConstraint{
		ConstraintName:         "fk_orders_users",
		UniqueConstraintSchema: "main",
		UniqueConstraintName:   "pk_users",
	}
	row := rc.ToSQL()
	if len(row) != 3 {
		t.Errorf("expected 3 columns, got %d", len(row))
	}
}

func TestREFERENTIALView_Constraints(t *testing.T) {
	rv := NewREFERENTIALView(&DS.BTree{})
	rows, err := rv.Constraints(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = rows
}

// --- Registry.QueryInformationSchema -----------------------------------------

func TestRegistry_QueryInformationSchema(t *testing.T) {
	r := NewRegistry(&DS.BTree{})

	views := []string{"columns", "tables", "views", "table_constraints", "referential_constraints"}
	for _, view := range views {
		rows, err := r.QueryInformationSchema(view, "", "")
		if err != nil {
			t.Errorf("unexpected error for %s: %v", view, err)
		}
		_ = rows
	}

	// Unknown view
	_, err := r.QueryInformationSchema("nonexistent", "", "")
	if err == nil {
		t.Error("expected error for unknown view")
	}
}

func TestRegistry_GetColumnNames_Unknown(t *testing.T) {
	r := NewRegistry(&DS.BTree{})
	_, err := r.GetColumnNames("nonexistent")
	if err == nil {
		t.Error("expected error for unknown view")
	}
}

// --- SchemaCache.InvalidateTable ---------------------------------------------

func TestSchemaCache_InvalidateTable(t *testing.T) {
	sc := NewSchemaCache()
	sc.Set("columns", []string{"a"}, nil)
	sc.InvalidateTable("users") // should flush all
	_, _, ok := sc.Get("columns")
	if ok {
		t.Error("expected cache to be cleared after InvalidateTable")
	}
}

// --- SchemaParser ------------------------------------------------------------

func newTestSchemaParser(t *testing.T) *SchemaParser {
	t.Helper()
	file, err := PB.OpenFile(":memory:", PB.O_RDWR|PB.O_CREATE)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	t.Cleanup(func() { file.Close() })
	pm, err := DS.NewPageManager(file, 4096)
	if err != nil {
		t.Fatalf("page manager: %v", err)
	}
	return NewSchemaParser(pm)
}

func TestSchemaParser_ParseSchema(t *testing.T) {
	sp := newTestSchemaParser(t)
	tables, cols, views, constraints, refs, err := sp.ParseSchema()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = tables
	_ = cols
	_ = views
	_ = constraints
	_ = refs
}

func TestSchemaParser_parseCreateTable(t *testing.T) {
	sp := newTestSchemaParser(t)
	ti, cols, constraints, err := sp.parseCreateTable("CREATE TABLE t (id INTEGER PRIMARY KEY)")
	// Not yet implemented — error expected
	if err == nil {
		t.Error("expected error from unimplemented parseCreateTable")
	}
	_ = ti
	_ = cols
	_ = constraints
}

func TestSchemaParser_parseCreateView(t *testing.T) {
	sp := newTestSchemaParser(t)
	vi, err := sp.parseCreateView("CREATE VIEW v AS SELECT 1")
	// Not yet implemented — error expected
	if err == nil {
		t.Error("expected error from unimplemented parseCreateView")
	}
	_ = vi
}
