package sqlvibe

import (
	"fmt"

	"github.com/sqlvibe/sqlvibe/internal/ds"
	"github.com/sqlvibe/sqlvibe/internal/pb"
	"github.com/sqlvibe/sqlvibe/internal/qe"
	"github.com/sqlvibe/sqlvibe/internal/qp"
)

type Database struct {
	pm     *ds.PageManager
	engine *qe.QueryEngine
	tx     *Transaction
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
	pos     int
}

type Statement struct {
	db     *Database
	sql    string
	parsed qp.ASTNode
}

type Transaction struct {
	db        *Database
	committed bool
	rollback  bool
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

func (db *Database) Prepare(sql string) (*Statement, error) {
	tokenizer := qp.NewTokenizer(sql)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, err
	}
	if len(tokens) == 0 {
		return nil, fmt.Errorf("empty SQL")
	}

	parser := qp.NewParser(tokens)
	ast, err := parser.Parse()
	if err != nil {
		return nil, err
	}
	if ast == nil {
		return nil, fmt.Errorf("failed to parse SQL")
	}

	return &Statement{db: db, sql: sql, parsed: ast}, nil
}

func (s *Statement) Exec(params ...interface{}) (Result, error) {
	return s.db.ExecWithParams(s.sql, params)
}

func (s *Statement) Query(params ...interface{}) (*Rows, error) {
	return s.db.QueryWithParams(s.sql, params)
}

func (s *Statement) Close() error {
	return nil
}

func (db *Database) ExecWithParams(sql string, params []interface{}) (Result, error) {
	return db.Exec(sql)
}

func (db *Database) QueryWithParams(sql string, params []interface{}) (*Rows, error) {
	return db.Query(sql)
}

func (db *Database) Begin() (*Transaction, error) {
	if db.tx != nil {
		return nil, fmt.Errorf("transaction already in progress")
	}
	db.tx = &Transaction{db: db, committed: false, rollback: false}
	return db.tx, nil
}

func (tx *Transaction) Commit() error {
	if tx.committed {
		return fmt.Errorf("transaction already committed")
	}
	if tx.rollback {
		return fmt.Errorf("transaction already rolled back")
	}
	tx.committed = true
	tx.db.tx = nil
	return nil
}

func (tx *Transaction) Rollback() error {
	if tx.committed {
		return fmt.Errorf("cannot rollback committed transaction")
	}
	tx.rollback = true
	tx.db.tx = nil
	return nil
}

func (tx *Transaction) Exec(sql string) (Result, error) {
	return tx.db.Exec(sql)
}

func (tx *Transaction) Query(sql string) (*Rows, error) {
	return tx.db.Query(sql)
}

func (db *Database) MustExec(sql string, params ...interface{}) Result {
	res, err := db.Exec(sql)
	if err != nil {
		panic(err)
	}
	return res
}
