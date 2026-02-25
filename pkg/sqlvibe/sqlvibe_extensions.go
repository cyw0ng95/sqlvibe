package sqlvibe

import (
	"sort"
	"strings"

	"github.com/cyw0ng95/sqlvibe/ext"
	"github.com/cyw0ng95/sqlvibe/internal/QP"
)

// querySqlvibeExtensions handles SELECT * FROM sqlvibe_extensions.
// It returns one row per registered extension with columns: name, description, functions.
func (db *Database) querySqlvibeExtensions(stmt *QP.SelectStmt) (*Rows, error) {
	columns := []string{"name", "description", "functions"}

	exts := ext.List()
	rows := make([][]interface{}, 0, len(exts))
	for _, e := range exts {
		funcs := e.Functions()
		// Sort function names for deterministic output.
		sorted := make([]string, len(funcs))
		copy(sorted, funcs)
		sort.Strings(sorted)
		rows = append(rows, []interface{}{
			e.Name(),
			e.Description(),
			strings.Join(sorted, ","),
		})
	}

	// Apply WHERE filter if present.
	if stmt.Where != nil {
		filtered := rows[:0]
		for _, row := range rows {
			rowMap := map[string]interface{}{
				"name":        row[0],
				"description": row[1],
				"functions":   row[2],
			}
			if db.evalWhereOnMap(stmt.Where, rowMap) {
				filtered = append(filtered, row)
			}
		}
		rows = filtered
	}

	// Apply column projection.
	projected, projCols, err := db.projectExtRows(rows, columns, stmt)
	if err != nil {
		return nil, err
	}

	return &Rows{Columns: projCols, Data: projected}, nil
}

// projectExtRows applies SELECT column projection to sqlvibe_extensions rows.
func (db *Database) projectExtRows(rows [][]interface{}, allCols []string, stmt *QP.SelectStmt) ([][]interface{}, []string, error) {
	// Check for SELECT *
	if len(stmt.Columns) == 1 {
		if cr, ok := stmt.Columns[0].(*QP.ColumnRef); ok && cr.Name == "*" {
			return rows, allCols, nil
		}
	}
	if len(stmt.Columns) == 0 {
		return rows, allCols, nil
	}

	// Map column name to index
	colIdx := make(map[string]int, len(allCols))
	for i, c := range allCols {
		colIdx[c] = i
	}

	projCols := make([]string, 0, len(stmt.Columns))
	colIndices := make([]int, 0, len(stmt.Columns))
	for _, col := range stmt.Columns {
		if cr, ok := col.(*QP.ColumnRef); ok {
			if cr.Name == "*" {
				for i, c := range allCols {
					projCols = append(projCols, c)
					colIndices = append(colIndices, i)
				}
				continue
			}
			idx, found := colIdx[strings.ToLower(cr.Name)]
			if !found {
				// Try case-insensitive
				for k, v := range colIdx {
					if strings.EqualFold(k, cr.Name) {
						idx = v
						found = true
						break
					}
				}
			}
			if found {
				projCols = append(projCols, cr.Name)
				colIndices = append(colIndices, idx)
			}
		}
	}

	projected := make([][]interface{}, len(rows))
	for i, row := range rows {
		newRow := make([]interface{}, len(colIndices))
		for j, idx := range colIndices {
			newRow[j] = row[idx]
		}
		projected[i] = newRow
	}
	return projected, projCols, nil
}
