package sqlvibe

import (
	"fmt"

	"github.com/sqlvibe/sqlvibe/internal/QP"
)

func (db *Database) applySetOp(left, right [][]interface{}, op string, all bool) [][]interface{} {
	switch op {
	case "UNION":
		return db.setOpUnion(left, right, all)
	case "EXCEPT":
		return db.setOpExcept(left, right, all)
	case "INTERSECT":
		return db.setOpIntersect(left, right, all)
	}
	return left
}

func (db *Database) setOpUnion(left, right [][]interface{}, all bool) [][]interface{} {
	if all {
		return append(left, right...)
	}
	seen := make(map[string]bool)
	result := make([][]interface{}, 0)
	for _, row := range left {
		key := db.rowKey(row)
		if !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}
	for _, row := range right {
		key := db.rowKey(row)
		if !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}
	return result
}

func (db *Database) setOpExcept(left, right [][]interface{}, all bool) [][]interface{} {
	rightSet := make(map[string]bool)
	for _, row := range right {
		rightSet[db.rowKey(row)] = true
	}
	result := make([][]interface{}, 0)
	for _, row := range left {
		key := db.rowKey(row)
		if !rightSet[key] {
			if all {
				result = append(result, row)
			} else {
				rightSet[key] = true
				result = append(result, row)
			}
		}
	}
	return result
}

func (db *Database) setOpIntersect(left, right [][]interface{}, all bool) [][]interface{} {
	rightSet := make(map[string]int)
	for _, row := range right {
		key := db.rowKey(row)
		rightSet[key]++
	}
	result := make([][]interface{}, 0)
	for _, row := range left {
		key := db.rowKey(row)
		if count, ok := rightSet[key]; ok && count > 0 {
			result = append(result, row)
			if !all {
				rightSet[key] = 0
			} else {
				rightSet[key]--
			}
		}
	}
	return result
}

func (db *Database) rowKey(row []interface{}) string {
	key := ""
	for i, v := range row {
		if i > 0 {
			key += "|"
		}
		key += fmt.Sprintf("%v", v)
	}
	return key
}

func (db *Database) execSetOp(stmt *QP.SelectStmt, originalSQL string) (*Rows, error) {
	// For now, use the existing direct execution path
	// This works but bypasses VM compilation for SetOps
	// TODO: Complete full VM bytecode compilation and merging

	// Create temporary left SELECT (without SetOp)
	leftStmt := *stmt
	leftStmt.SetOp = ""
	leftStmt.SetOpAll = false
	leftStmt.SetOpRight = nil

	// Execute left side through VM if possible, otherwise direct
	var leftRows *Rows
	var err error
	if leftStmt.From != nil {
		leftRows, err = db.execVMQuery("", &leftStmt)
	} else {
		// Handle SELECT without FROM
		leftRows = &Rows{Columns: []string{}, Data: [][]interface{}{}}
	}
	if err != nil {
		return nil, fmt.Errorf("SetOp left side error: %w", err)
	}

	// Execute right side
	var rightRows *Rows
	if stmt.SetOpRight != nil {
		if stmt.SetOpRight.From != nil {
			rightRows, err = db.execVMQuery("", stmt.SetOpRight)
		} else {
			rightRows = &Rows{Columns: []string{}, Data: [][]interface{}{}}
		}
		if err != nil {
			return nil, fmt.Errorf("SetOp right side error: %w", err)
		}
	}

	// Apply set operation using existing functions
	result := db.applySetOp(leftRows.Data, rightRows.Data, stmt.SetOp, stmt.SetOpAll)

	return &Rows{
		Columns: leftRows.Columns,
		Data:    result,
	}, nil
}
