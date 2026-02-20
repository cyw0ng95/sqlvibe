package sqlvibe

import (
	"fmt"
	"strings"

	"github.com/sqlvibe/sqlvibe/internal/CG"
	"github.com/sqlvibe/sqlvibe/internal/QP"
	"github.com/sqlvibe/sqlvibe/internal/VM"
)

func (db *Database) handleExplain(stmt *QP.ExplainStmt, sql string) (*Rows, error) {
	if stmt.QueryPlan {
		return db.explainQueryPlan(stmt)
	}

	sqlType := stmt.Query.NodeType()
	if sqlType == "SelectStmt" {
		// Strip "EXPLAIN" prefix from SQL and compile
		innerSQL := strings.TrimPrefix(sql, "EXPLAIN ")
		innerSQL = strings.TrimPrefix(innerSQL, "EXPLAIN")
		innerSQL = strings.TrimSpace(innerSQL)

		program, err := CG.CompileWithSchema(innerSQL, nil)
		if err != nil {
			return nil, err
		}
		return db.explainProgram(program)
	}
	return &Rows{Columns: []string{"opcode"}, Data: [][]interface{}{}}, nil
}

// explainQueryPlan generates a QUERY PLAN output similar to SQLite's EXPLAIN QUERY PLAN.
func (db *Database) explainQueryPlan(stmt *QP.ExplainStmt) (*Rows, error) {
	cols := []string{"QUERY PLAN"}
	rows := make([][]interface{}, 0)

	if stmt.Query.NodeType() == "SelectStmt" {
		sel := stmt.Query.(*QP.SelectStmt)
		planLines := db.buildSelectPlan(sel)
		for _, line := range planLines {
			rows = append(rows, []interface{}{line})
		}
	} else {
		rows = append(rows, []interface{}{"|--" + strings.ToUpper(stmt.Query.NodeType())})
	}

	return &Rows{Columns: cols, Data: rows}, nil
}

// buildSelectPlan builds plan lines for a SELECT statement.
func (db *Database) buildSelectPlan(sel *QP.SelectStmt) []string {
	var lines []string

	if sel.From != nil {
		lines = append(lines, db.buildTableRefPlan(sel.From, sel.Where)...)
	}

	if len(sel.GroupBy) > 0 {
		lines = append(lines, "|--USE TEMP B-TREE (GROUP BY)")
	}

	if len(sel.OrderBy) > 0 {
		lines = append(lines, "|--USE TEMP B-TREE (ORDER BY)")
	}

	return lines
}

// buildTableRefPlan builds plan lines for a table reference (may be a join).
func (db *Database) buildTableRefPlan(ref *QP.TableRef, where QP.Expr) []string {
	var lines []string

	if ref.Subquery != nil {
		alias := ref.Alias
		if alias == "" {
			alias = "subquery"
		}
		lines = append(lines, "|--SCAN "+alias+" (subquery)")
		return lines
	}

	if ref.Join == nil {
		// Single table, no join.
		lines = append(lines, db.tableAccessLine(ref, where))
		return lines
	}

	// Walk the join chain iteratively to avoid cycles.
	// The parser stores: ref.Join = {Left: ref, Right: next}, then ref = next
	// So we collect the first table, then follow .Join.Right repeatedly.
	cur := ref
	visited := make(map[*QP.TableRef]bool)
	for cur != nil && !visited[cur] {
		visited[cur] = true
		lines = append(lines, db.tableAccessLine(cur, where))
		if cur.Join != nil {
			cur = cur.Join.Right
		} else {
			break
		}
	}
	return lines
}

// tableAccessLine produces a single SCAN/SEARCH line for a table.
func (db *Database) tableAccessLine(ref *QP.TableRef, where QP.Expr) string {
	name := ref.Name
	if ref.Alias != "" {
		name = ref.Alias
	}

	// Check if WHERE references this table's primary key (rowid).
	if where != nil && db.whereUsesPrimaryKey(ref.Name, where) {
		return "|--SEARCH " + name + " USING INTEGER PRIMARY KEY (rowid=?)"
	}
	return "|--SCAN " + name
}

// whereUsesPrimaryKey returns true when the WHERE clause is an equality on the
// named table's "id" column (treated as the primary key / rowid).
func (db *Database) whereUsesPrimaryKey(tableName string, where QP.Expr) bool {
	bin, ok := where.(*QP.BinaryExpr)
	if !ok {
		return false
	}

	// Only equality comparisons qualify as primary-key lookups.
	if bin.Op != QP.TokenEq {
		return false
	}

	colRef, ok := bin.Left.(*QP.ColumnRef)
	if !ok {
		return false
	}

	// Accept "id" or "<table>.id" as a primary key reference.
	colName := strings.ToLower(colRef.Name)
	return colName == "id" && (colRef.Table == "" || strings.EqualFold(colRef.Table, tableName))
}

func (db *Database) explainProgram(program *VM.Program) (*Rows, error) {
	if program == nil || len(program.Instructions) == 0 {
		return &Rows{Columns: []string{"result"}, Data: [][]interface{}{{"no bytecode generated"}}}, nil
	}

	cols := []string{"addr", "opcode", "p1", "p2", "p3", "p4", "comment"}
	rows := make([][]interface{}, 0)

	for i, inst := range program.Instructions {
		row := []interface{}{
			i,
			VM.OpCodeInfo[inst.Op],
			inst.P1,
			inst.P2,
			inst.P3,
			fmt.Sprintf("%v", inst.P4),
			"",
		}
		rows = append(rows, row)
	}

	return &Rows{Columns: cols, Data: rows}, nil
}
