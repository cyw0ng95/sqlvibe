package sqlvibe

import (
	"github.com/sqlvibe/sqlvibe/internal/ds"
	"github.com/sqlvibe/sqlvibe/internal/pb"
	"github.com/sqlvibe/sqlvibe/internal/qe"
	"github.com/sqlvibe/sqlvibe/internal/qp"
)

type Database struct {
	pm     *ds.PageManager
	engine *qe.QueryEngine
}

type Conn interface {
	Exec(sql string) (Result, error)
	Query(sql string) (*Rows, error)
	Close() error
}

type Result struct {
	LastInsertRowID int64
	RowsAffected    int64
}

type Rows struct {
	Columns []string
	Data    [][]interface{}
}

func Open(path string) (*Database, error) {
	file, err := pb.OpenFile(path, pb.O_RDWR|pb.O_CREATE)
	if err != nil {
		return nil, err
	}

	pm, err := ds.NewPageManager(file, 4096)
	if err != nil {
		file.Close()
		return nil, err
	}

	engine := qe.NewQueryEngine()

	return &Database{
		pm:     pm,
		engine: engine,
	}, nil
}

func (db *Database) Exec(sql string) (Result, error) {
	tokenizer := qp.NewTokenizer(sql)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return Result{}, err
	}
	if len(tokens) == 0 {
		return Result{}, nil
	}

	parser := qp.NewParser(tokens)
	ast, err := parser.Parse()
	if err != nil {
		return Result{}, err
	}
	if ast == nil {
		return Result{}, nil
	}

	switch ast.NodeType() {
	case "CreateTableStmt":
		stmt := ast.(*qp.CreateTableStmt)
		schema := make(map[string]qe.ColumnType)
		for _, col := range stmt.Columns {
			schema[col.Name] = qe.ColumnType{Name: col.Name, Type: col.Type}
		}
		db.engine.RegisterTable(stmt.Name, schema)
		return Result{}, nil
	case "InsertStmt":
		stmt := ast.(*qp.InsertStmt)
		return Result{RowsAffected: int64(len(stmt.Values))}, nil
	case "UpdateStmt":
		return Result{RowsAffected: 0}, nil
	case "DeleteStmt":
		return Result{RowsAffected: 0}, nil
	case "DropTableStmt":
		return Result{}, nil
	}

	return Result{}, nil
}

func (db *Database) Query(sql string) (*Rows, error) {
	tokenizer := qp.NewTokenizer(sql)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, err
	}
	if len(tokens) == 0 {
		return nil, nil
	}

	parser := qp.NewParser(tokens)
	ast, err := parser.Parse()
	if err != nil {
		return nil, err
	}
	if ast == nil {
		return nil, nil
	}

	if ast.NodeType() == "SelectStmt" {
		stmt := ast.(*qp.SelectStmt)
		cols := make([]string, 0)
		for _, col := range stmt.Columns {
			if cr, ok := col.(*qp.ColumnRef); ok {
				cols = append(cols, cr.Name)
			} else {
				cols = append(cols, "expr")
			}
		}
		return &Rows{Columns: cols, Data: nil}, nil
	}

	return nil, nil
}

func (db *Database) Close() error {
	return db.pm.Close()
}
