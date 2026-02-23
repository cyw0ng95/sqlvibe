package sqlvibe

import (
	"fmt"
	"strings"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
)

// maxTriggerRecursionDepth is the maximum number of nested trigger invocations
// allowed before aborting with a recursion limit error.
const maxTriggerRecursionDepth = 16

// ---- AUTOINCREMENT ----

// autoAssignAutoincrement assigns a monotonically increasing PK value to a row
// when the table has AUTOINCREMENT enabled and the PK column is nil.
// Unlike autoAssignPK, this never reuses IDs even after DELETE.
func (db *Database) autoAssignAutoincrement(tableName string, row map[string]interface{}) {
	pkCol, ok := db.autoincrement[tableName]
	if !ok {
		return
	}
	if v := row[pkCol]; v != nil {
		// Explicit value supplied – update the sequence if it's larger
		if id, ok := toInt64(v); ok {
			if id > db.seqValues[tableName] {
				db.seqValues[tableName] = id
			}
		}
		return
	}
	// Assign next value
	db.seqValues[tableName]++
	row[pkCol] = db.seqValues[tableName]
}

// ---- FOREIGN KEY ENFORCEMENT ----

// checkFKOnInsert verifies all FK constraints for the table when a new row is inserted.
func (db *Database) checkFKOnInsert(tableName string, row map[string]interface{}) error {
	if !db.foreignKeysEnabled {
		return nil
	}
	fks := db.foreignKeys[tableName]
	for _, fk := range fks {
		if err := db.enforceFKInsert(fk, row); err != nil {
			return err
		}
	}
	return nil
}

// enforceFKInsert checks that for each FK the referenced row exists in the parent table.
func (db *Database) enforceFKInsert(fk QP.ForeignKeyConstraint, row map[string]interface{}) error {
	// If all child columns are NULL, FK is satisfied (nullable FK)
	allNull := true
	for _, col := range fk.ChildColumns {
		if row[col] != nil {
			allNull = false
			break
		}
	}
	if allNull {
		return nil
	}
	// Look for a matching row in the parent table
	parentRows := db.data[fk.ParentTable]
	parentPKCols := fk.ParentColumns
	if len(parentPKCols) == 0 {
		parentPKCols = db.primaryKeys[fk.ParentTable]
	}
	for _, pr := range parentRows {
		match := true
		for i, childCol := range fk.ChildColumns {
			if i >= len(parentPKCols) {
				break
			}
			cv := row[childCol]
			pv := pr[parentPKCols[i]]
			if compareInterfaceVals(cv, pv) != 0 {
				match = false
				break
			}
		}
		if match {
			return nil
		}
	}
	return fmt.Errorf("FOREIGN KEY constraint failed: parent table %s", fk.ParentTable)
}

// checkFKOnDelete verifies FK constraints before deleting a row from a parent table.
// Handles ON DELETE CASCADE / RESTRICT / SET NULL.
func (db *Database) checkFKOnDelete(tableName string, row map[string]interface{}) error {
	if !db.foreignKeysEnabled {
		return nil
	}
	// Find all child tables that reference this table
	for childTable, fks := range db.foreignKeys {
		for _, fk := range fks {
			if fk.ParentTable != tableName {
				continue
			}
			parentCols := fk.ParentColumns
			if len(parentCols) == 0 {
				parentCols = db.primaryKeys[tableName]
			}
			switch fk.OnDelete {
			case QP.ReferenceCascade:
				if err := db.cascadeDelete(childTable, fk, parentCols, row); err != nil {
					return err
				}
			case QP.ReferenceSetNull:
				db.cascadeSetNull(childTable, fk, parentCols, row)
			case QP.ReferenceSetDefault:
				db.cascadeSetNull(childTable, fk, parentCols, row) // simplified: set to null
			default: // NoAction / Restrict
				if db.hasChildRows(childTable, fk, parentCols, row) {
					return fmt.Errorf("FOREIGN KEY constraint failed: %s -> %s", childTable, tableName)
				}
			}
		}
	}
	return nil
}

// checkFKOnUpdate verifies FK constraints before updating a row in a parent table.
// Handles ON UPDATE CASCADE / RESTRICT / SET NULL.
func (db *Database) checkFKOnUpdate(tableName string, oldRow, newRow map[string]interface{}) error {
	if !db.foreignKeysEnabled {
		return nil
	}
	for childTable, fks := range db.foreignKeys {
		for _, fk := range fks {
			if fk.ParentTable != tableName {
				continue
			}
			parentCols := fk.ParentColumns
			if len(parentCols) == 0 {
				parentCols = db.primaryKeys[tableName]
			}
			// Check if the referenced value actually changed
			changed := false
			for _, pc := range parentCols {
				if compareInterfaceVals(oldRow[pc], newRow[pc]) != 0 {
					changed = true
					break
				}
			}
			if !changed {
				continue
			}
			switch fk.OnUpdate {
			case QP.ReferenceCascade:
				db.cascadeUpdate(childTable, fk, parentCols, oldRow, newRow)
			case QP.ReferenceSetNull:
				db.cascadeSetNull(childTable, fk, parentCols, oldRow)
			default: // NoAction / Restrict
				if db.hasChildRows(childTable, fk, parentCols, oldRow) {
					return fmt.Errorf("FOREIGN KEY constraint failed: %s -> %s", childTable, tableName)
				}
			}
		}
	}
	return nil
}

func (db *Database) hasChildRows(childTable string, fk QP.ForeignKeyConstraint, parentCols []string, parentRow map[string]interface{}) bool {
	for _, cr := range db.data[childTable] {
		if rowMatchesFK(cr, fk.ChildColumns, parentRow, parentCols) {
			return true
		}
	}
	return false
}

func (db *Database) cascadeDelete(childTable string, fk QP.ForeignKeyConstraint, parentCols []string, parentRow map[string]interface{}) error {
	rows := db.data[childTable]
	toDelete := make([]int, 0)
	for i, cr := range rows {
		if rowMatchesFK(cr, fk.ChildColumns, parentRow, parentCols) {
			toDelete = append(toDelete, i)
		}
	}
	// Delete in reverse order to keep indices valid.
	// Before removing each row, recursively cascade to its own children.
	for i := len(toDelete) - 1; i >= 0; i-- {
		idx := toDelete[i]
		row := db.data[childTable][idx]
		// Recursively enforce FK constraints on the child being deleted
		// (handles 3+ level ON DELETE CASCADE chains).
		if err := db.checkFKOnDelete(childTable, row); err != nil {
			return err
		}
		db.removeFromIndexes(childTable, row, idx)
		db.data[childTable] = append(db.data[childTable][:idx], db.data[childTable][idx+1:]...)
	}
	return nil
}

func (db *Database) cascadeSetNull(childTable string, fk QP.ForeignKeyConstraint, parentCols []string, parentRow map[string]interface{}) {
	for i, cr := range db.data[childTable] {
		if rowMatchesFK(cr, fk.ChildColumns, parentRow, parentCols) {
			updated := make(map[string]interface{}, len(cr))
			for k, v := range cr {
				updated[k] = v
			}
			for _, cc := range fk.ChildColumns {
				updated[cc] = nil
			}
			db.data[childTable][i] = updated
		}
	}
}

func (db *Database) cascadeUpdate(childTable string, fk QP.ForeignKeyConstraint, parentCols []string, oldParentRow, newParentRow map[string]interface{}) {
	for i, cr := range db.data[childTable] {
		if rowMatchesFK(cr, fk.ChildColumns, oldParentRow, parentCols) {
			updated := make(map[string]interface{}, len(cr))
			for k, v := range cr {
				updated[k] = v
			}
			for j, cc := range fk.ChildColumns {
				if j < len(parentCols) {
					updated[cc] = newParentRow[parentCols[j]]
				}
			}
			db.data[childTable][i] = updated
		}
	}
}

func rowMatchesFK(row map[string]interface{}, childCols []string, parentRow map[string]interface{}, parentCols []string) bool {
	for i, cc := range childCols {
		if i >= len(parentCols) {
			return false
		}
		if compareInterfaceVals(row[cc], parentRow[parentCols[i]]) != 0 {
			return false
		}
	}
	return true
}

// compareInterfaceVals compares two values numerically if possible, otherwise string-compares.
func compareInterfaceVals(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	ai, aIsInt := toInt64(a)
	bi, bIsInt := toInt64(b)
	if aIsInt && bIsInt {
		if ai < bi {
			return -1
		} else if ai > bi {
			return 1
		}
		return 0
	}
	af, aIsFloat := toFloat64(a)
	bf, bIsFloat := toFloat64(b)
	if aIsFloat && bIsFloat {
		if af < bf {
			return -1
		} else if af > bf {
			return 1
		}
		return 0
	}
	as := fmt.Sprintf("%v", a)
	bs := fmt.Sprintf("%v", b)
	return strings.Compare(as, bs)
}

func toInt64(v interface{}) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int:
		return int64(x), true
	case int32:
		return int64(x), true
	}
	return 0, false
}

func toFloat64(v interface{}) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int64:
		return float64(x), true
	case int:
		return float64(x), true
	}
	return 0, false
}

// sqlNullLiteral is the SQL NULL literal string used by QUOTE() and triggerExprToSQL.
const sqlNullLiteral = "NULL"

// ---- TRIGGER FIRING ----

// fireTriggers executes triggers for the given table, event (INSERT/UPDATE/DELETE), and timing (BEFORE/AFTER).
// oldRow may be nil for INSERT events; newRow may be nil for DELETE events.
func (db *Database) fireTriggers(tableName, event, timing string, oldRow, newRow map[string]interface{}, depth int) error {
	if depth > maxTriggerRecursionDepth {
		return fmt.Errorf("trigger recursion limit exceeded")
	}
	trigs := db.triggers[tableName]
	for _, t := range trigs {
		if !strings.EqualFold(t.Time, timing) {
			continue
		}
		if !strings.EqualFold(t.Event, event) {
			continue
		}
		// UPDATE OF column filter
		if len(t.Columns) > 0 && strings.EqualFold(event, "UPDATE") && oldRow != nil && newRow != nil {
			changed := false
			for _, col := range t.Columns {
				if compareInterfaceVals(oldRow[col], newRow[col]) != 0 {
					changed = true
					break
				}
			}
			if !changed {
				continue
			}
		}
		// Evaluate WHEN condition
		if t.When != nil {
			row := newRow
			if row == nil {
				row = oldRow
			}
			if row == nil {
				row = make(map[string]interface{})
			}
			fullRow := db.buildTriggerRow(oldRow, newRow, row)
			dbCtx := &dbVmContext{db: db}
			result := dbCtx.evaluateCheckConstraint(t.When, fullRow)
			if !isTruthy(result) {
				continue
			}
		}
		// Execute trigger body
		for _, bodyStmt := range t.Body {
			sqlStr := db.triggerBodySQL(bodyStmt, oldRow, newRow)
			if sqlStr == "" {
				continue
			}
			if _, err := db.Exec(sqlStr); err != nil {
				return fmt.Errorf("trigger %s body error: %w", t.Name, err)
			}
		}
	}
	return nil
}

// buildTriggerRow merges OLD, NEW, and row data for trigger WHEN evaluation.
func (db *Database) buildTriggerRow(oldRow, newRow, baseRow map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range baseRow {
		result[k] = v
	}
	if oldRow != nil {
		for k, v := range oldRow {
			result["OLD."+k] = v
		}
	}
	if newRow != nil {
		for k, v := range newRow {
			result["NEW."+k] = v
		}
	}
	return result
}

// triggerBodySQL converts a trigger body statement AST node back to SQL string for execution.
// This is a simplified approach using the AST structure directly.
func (db *Database) triggerBodySQL(stmt QP.ASTNode, oldRow, newRow map[string]interface{}) string {
	switch s := stmt.(type) {
	case *QP.InsertStmt:
		return db.rewriteTriggerInsert(s, oldRow, newRow)
	case *QP.UpdateStmt:
		return db.rewriteTriggerUpdate(s, oldRow, newRow)
	case *QP.DeleteStmt:
		return db.rewriteTriggerDelete(s, oldRow, newRow)
	}
	return ""
}

// rewriteTriggerInsert reconstructs an INSERT SQL string with NEW/OLD substituted.
func (db *Database) rewriteTriggerInsert(s *QP.InsertStmt, oldRow, newRow map[string]interface{}) string {
	colParts := make([]string, len(s.Columns))
	copy(colParts, s.Columns)
	var valRows []string
	for _, valRow := range s.Values {
		parts := make([]string, len(valRow))
		for i, expr := range valRow {
			parts[i] = triggerExprToSQL(expr, oldRow, newRow)
		}
		valRows = append(valRows, "("+strings.Join(parts, ", ")+")")
	}
	if len(colParts) > 0 {
		return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
			s.Table, strings.Join(colParts, ", "), strings.Join(valRows, ", "))
	}
	return fmt.Sprintf("INSERT INTO %s VALUES %s",
		s.Table, strings.Join(valRows, ", "))
}

// rewriteTriggerUpdate reconstructs an UPDATE SQL string.
func (db *Database) rewriteTriggerUpdate(s *QP.UpdateStmt, oldRow, newRow map[string]interface{}) string {
	sets := make([]string, len(s.Set))
	for i, sc := range s.Set {
		colName := triggerExprToSQL(sc.Column, oldRow, newRow)
		val := triggerExprToSQL(sc.Value, oldRow, newRow)
		sets[i] = colName + " = " + val
	}
	sql := fmt.Sprintf("UPDATE %s SET %s", s.Table, strings.Join(sets, ", "))
	if s.Where != nil {
		sql += " WHERE " + triggerExprToSQL(s.Where, oldRow, newRow)
	}
	return sql
}

// rewriteTriggerDelete reconstructs a DELETE SQL string.
func (db *Database) rewriteTriggerDelete(s *QP.DeleteStmt, oldRow, newRow map[string]interface{}) string {
	sql := fmt.Sprintf("DELETE FROM %s", s.Table)
	if s.Where != nil {
		sql += " WHERE " + triggerExprToSQL(s.Where, oldRow, newRow)
	}
	return sql
}

// exprToSQL converts an expression AST node to a SQL string, substituting NEW/OLD references.
func triggerExprToSQL(expr QP.Expr, oldRow, newRow map[string]interface{}) string {
	if expr == nil {
		return sqlNullLiteral
	}
	switch e := expr.(type) {
	case *QP.Literal:
		return literalToString(e.Value)
	case *QP.ColumnRef:
		// Check for NEW.col / OLD.col
		tableName := strings.ToUpper(e.Table)
		if tableName == "NEW" && newRow != nil {
			if v, ok := newRow[e.Name]; ok {
				return literalToString(v)
			}
			return sqlNullLiteral
		}
		if tableName == "OLD" && oldRow != nil {
			if v, ok := oldRow[e.Name]; ok {
				return literalToString(v)
			}
			return sqlNullLiteral
		}
		if e.Table != "" {
			return e.Table + "." + e.Name
		}
		return e.Name
	case *QP.BinaryExpr:
		left := triggerExprToSQL(e.Left, oldRow, newRow)
		right := triggerExprToSQL(e.Right, oldRow, newRow)
		op := tokenTypeToSQL(e.Op)
		return "(" + left + " " + op + " " + right + ")"
	case *QP.FuncCall:
		args := make([]string, len(e.Args))
		for i, a := range e.Args {
			args[i] = triggerExprToSQL(a, oldRow, newRow)
		}
		return e.Name + "(" + strings.Join(args, ", ") + ")"
	}
	return sqlNullLiteral
}

func tokenTypeToSQL(t QP.TokenType) string {
	switch t {
	case QP.TokenEq:
		return "="
	case QP.TokenNe:
		return "!="
	case QP.TokenLt:
		return "<"
	case QP.TokenLe:
		return "<="
	case QP.TokenGt:
		return ">"
	case QP.TokenGe:
		return ">="
	case QP.TokenAnd:
		return "AND"
	case QP.TokenOr:
		return "OR"
	case QP.TokenPlus:
		return "+"
	case QP.TokenMinus:
		return "-"
	case QP.TokenAsterisk:
		return "*"
	case QP.TokenSlash:
		return "/"
	}
	return "="
}
