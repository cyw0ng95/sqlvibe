package sqlvibe

import (
	"fmt"
	"strings"

	"github.com/sqlvibe/sqlvibe/internal/CG"
	"github.com/sqlvibe/sqlvibe/internal/QP"
	"github.com/sqlvibe/sqlvibe/internal/VM"
)

func (db *Database) handleExplain(stmt *QP.ExplainStmt, sql string) (*Rows, error) {
	sqlType := stmt.Query.NodeType()
	if sqlType == "SelectStmt" {
		sel := stmt.Query.(*QP.SelectStmt)

		// Get table column map
		var tableColMap map[string]int
		if sel.From != nil {
			tableName := sel.From.Name
			if db.data[tableName] != nil {
				cols := db.columnOrder[tableName]
				tableColMap = make(map[string]int)
				for i, col := range cols {
					tableColMap[col] = i
				}
			}
		}

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
