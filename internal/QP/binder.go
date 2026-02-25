package QP

import (
	"fmt"
	"strings"
)

// ErrMissingParam is returned when a positional or named parameter is not provided.
var ErrMissingParam = fmt.Errorf("missing parameter")

// BindParams walks an AST tree and replaces PlaceholderExpr nodes with concrete
// Literal values from params (positional) or namedParams (named).
//
// Positional params are bound in left-to-right order.
// Named params are looked up in namedParams with the leading ':' or '@' stripped.
// Returns ErrMissingParam if a positional index is out of range or a named key is absent.
// Extra positional params beyond the number of placeholders are silently ignored (SQLite behaviour).
func BindParams(node ASTNode, params []interface{}, namedParams map[string]interface{}) (ASTNode, error) {
	idx := 0
	return bindNode(node, params, namedParams, &idx)
}

// bindNode recursively walks and binds parameters in any ASTNode.
func bindNode(node ASTNode, params []interface{}, namedParams map[string]interface{}, idx *int) (ASTNode, error) {
	if node == nil {
		return nil, nil
	}
	switch n := node.(type) {
	case *SelectStmt:
		return bindSelectStmt(n, params, namedParams, idx)
	case *InsertStmt:
		return bindInsertStmt(n, params, namedParams, idx)
	case *UpdateStmt:
		return bindUpdateStmt(n, params, namedParams, idx)
	case *DeleteStmt:
		return bindDeleteStmt(n, params, namedParams, idx)
	default:
		return node, nil
	}
}

// bindSelectStmt binds parameters within a SELECT statement.
func bindSelectStmt(stmt *SelectStmt, params []interface{}, namedParams map[string]interface{}, idx *int) (*SelectStmt, error) {
	if stmt == nil {
		return nil, nil
	}
	// Bind SELECT columns
	for i, col := range stmt.Columns {
		bound, err := bindExpr(col, params, namedParams, idx)
		if err != nil {
			return nil, err
		}
		stmt.Columns[i] = bound
	}
	// Bind WHERE
	if stmt.Where != nil {
		bound, err := bindExpr(stmt.Where, params, namedParams, idx)
		if err != nil {
			return nil, err
		}
		stmt.Where = bound
	}
	// Bind HAVING
	if stmt.Having != nil {
		bound, err := bindExpr(stmt.Having, params, namedParams, idx)
		if err != nil {
			return nil, err
		}
		stmt.Having = bound
	}
	return stmt, nil
}

// bindInsertStmt binds parameters within an INSERT statement.
func bindInsertStmt(stmt *InsertStmt, params []interface{}, namedParams map[string]interface{}, idx *int) (*InsertStmt, error) {
	if stmt == nil {
		return nil, nil
	}
	for ri, row := range stmt.Values {
		for vi, val := range row {
			bound, err := bindExpr(val, params, namedParams, idx)
			if err != nil {
				return nil, err
			}
			stmt.Values[ri][vi] = bound
		}
	}
	return stmt, nil
}

// bindUpdateStmt binds parameters within an UPDATE statement.
func bindUpdateStmt(stmt *UpdateStmt, params []interface{}, namedParams map[string]interface{}, idx *int) (*UpdateStmt, error) {
	if stmt == nil {
		return nil, nil
	}
	for i, set := range stmt.Set {
		bound, err := bindExpr(set.Value, params, namedParams, idx)
		if err != nil {
			return nil, err
		}
		stmt.Set[i].Value = bound
	}
	if stmt.Where != nil {
		bound, err := bindExpr(stmt.Where, params, namedParams, idx)
		if err != nil {
			return nil, err
		}
		stmt.Where = bound
	}
	return stmt, nil
}

// bindDeleteStmt binds parameters within a DELETE statement.
func bindDeleteStmt(stmt *DeleteStmt, params []interface{}, namedParams map[string]interface{}, idx *int) (*DeleteStmt, error) {
	if stmt == nil {
		return nil, nil
	}
	if stmt.Where != nil {
		bound, err := bindExpr(stmt.Where, params, namedParams, idx)
		if err != nil {
			return nil, err
		}
		stmt.Where = bound
	}
	return stmt, nil
}

// bindExpr recursively binds parameters within an expression.
func bindExpr(expr Expr, params []interface{}, namedParams map[string]interface{}, idx *int) (Expr, error) {
	if expr == nil {
		return nil, nil
	}
	switch e := expr.(type) {
	case *PlaceholderExpr:
		return bindPlaceholder(e, params, namedParams, idx)
	case *BinaryExpr:
		left, err := bindExpr(e.Left, params, namedParams, idx)
		if err != nil {
			return nil, err
		}
		right, err := bindExpr(e.Right, params, namedParams, idx)
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Op: e.Op, Left: left, Right: right}, nil
	case *UnaryExpr:
		inner, err := bindExpr(e.Expr, params, namedParams, idx)
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: e.Op, Expr: inner}, nil
	case *FuncCall:
		args := make([]Expr, len(e.Args))
		for i, arg := range e.Args {
			bound, err := bindExpr(arg, params, namedParams, idx)
			if err != nil {
				return nil, err
			}
			args[i] = bound
		}
		return &FuncCall{Name: e.Name, Args: args, Distinct: e.Distinct}, nil
	case *AliasExpr:
		inner, err := bindExpr(e.Expr, params, namedParams, idx)
		if err != nil {
			return nil, err
		}
		return &AliasExpr{Expr: inner, Alias: e.Alias}, nil
	case *CaseExpr:
		var operand Expr
		var err error
		if e.Operand != nil {
			operand, err = bindExpr(e.Operand, params, namedParams, idx)
			if err != nil {
				return nil, err
			}
		}
		whens := make([]CaseWhen, len(e.Whens))
		for i, w := range e.Whens {
			cond, err := bindExpr(w.Condition, params, namedParams, idx)
			if err != nil {
				return nil, err
			}
			res, err := bindExpr(w.Result, params, namedParams, idx)
			if err != nil {
				return nil, err
			}
			whens[i] = CaseWhen{Condition: cond, Result: res}
		}
		var elseExpr Expr
		if e.Else != nil {
			elseExpr, err = bindExpr(e.Else, params, namedParams, idx)
			if err != nil {
				return nil, err
			}
		}
		return &CaseExpr{Operand: operand, Whens: whens, Else: elseExpr}, nil
	default:
		return expr, nil
	}
}

// bindPlaceholder resolves a single PlaceholderExpr to a Literal.
func bindPlaceholder(e *PlaceholderExpr, params []interface{}, namedParams map[string]interface{}, idx *int) (Expr, error) {
	if e.Positional {
		if *idx >= len(params) {
			return nil, fmt.Errorf("%w: positional parameter %d not provided", ErrMissingParam, *idx+1)
		}
		val := params[*idx]
		*idx++
		return &Literal{Value: toBindValue(val)}, nil
	}
	// Named parameter: strip leading ':' or '@'
	key := strings.TrimLeft(e.Name, ":@")
	if namedParams == nil {
		return nil, fmt.Errorf("%w: named parameter %s not provided", ErrMissingParam, e.Name)
	}
	val, ok := namedParams[key]
	if !ok {
		return nil, fmt.Errorf("%w: named parameter %s not provided", ErrMissingParam, e.Name)
	}
	return &Literal{Value: toBindValue(val)}, nil
}

// toBindValue converts a Go value to a type suitable for a Literal node.
func toBindValue(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case int:
		return int64(val)
	case int8:
		return int64(val)
	case int16:
		return int64(val)
	case int32:
		return int64(val)
	case int64:
		return val
	case uint:
		return int64(val)
	case uint8:
		return int64(val)
	case uint16:
		return int64(val)
	case uint32:
		return int64(val)
	case uint64:
		// Values that fit in int64 are converted; larger values use string representation.
		const maxInt64 uint64 = 1<<63 - 1
		if val <= maxInt64 {
			return int64(val)
		}
		return fmt.Sprintf("%d", val)
	case float32:
		return float64(val)
	case float64:
		return val
	case string:
		return val
	case []byte:
		return val
	case bool:
		if val {
			return int64(1)
		}
		return int64(0)
	default:
		return fmt.Sprintf("%v", val)
	}
}
