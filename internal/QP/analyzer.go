package QP

// RequiredColumns extracts all column names referenced by a SelectStmt.
// It covers SELECT, WHERE, ORDER BY, and GROUP BY clauses.
func RequiredColumns(stmt *SelectStmt) []string {
	required := make(map[string]bool)
	for _, col := range stmt.Columns {
		for _, name := range colNamesFromExpr(col) {
			required[name] = true
		}
	}
	for _, name := range colNamesFromExpr(stmt.Where) {
		required[name] = true
	}
	for _, ob := range stmt.OrderBy {
		for _, name := range colNamesFromExpr(ob.Expr) {
			required[name] = true
		}
	}
	for _, gb := range stmt.GroupBy {
		for _, name := range colNamesFromExpr(gb) {
			required[name] = true
		}
	}
	out := make([]string, 0, len(required))
	for k := range required {
		out = append(out, k)
	}
	return out
}

// colNamesFromExpr recursively collects ColumnRef names from an expression tree.
func colNamesFromExpr(e Expr) []string {
	if e == nil {
		return nil
	}
	switch v := e.(type) {
	case *ColumnRef:
		return []string{v.Name}
	case *BinaryExpr:
		return append(colNamesFromExpr(v.Left), colNamesFromExpr(v.Right)...)
	case *UnaryExpr:
		return colNamesFromExpr(v.Expr)
	case *FuncCall:
		var names []string
		for _, arg := range v.Args {
			names = append(names, colNamesFromExpr(arg)...)
		}
		return names
	case *AliasExpr:
		return colNamesFromExpr(v.Expr)
	case *CaseExpr:
		var names []string
		names = append(names, colNamesFromExpr(v.Operand)...)
		for _, w := range v.Whens {
			names = append(names, colNamesFromExpr(w.Condition)...)
			names = append(names, colNamesFromExpr(w.Result)...)
		}
		names = append(names, colNamesFromExpr(v.Else)...)
		return names
	case *CastExpr:
		return colNamesFromExpr(v.Expr)
	case *WindowFuncExpr:
		var names []string
		for _, arg := range v.Args {
			names = append(names, colNamesFromExpr(arg)...)
		}
		for _, p := range v.Partition {
			names = append(names, colNamesFromExpr(p)...)
		}
		for _, ob := range v.OrderBy {
			names = append(names, colNamesFromExpr(ob.Expr)...)
		}
		return names
	case *AnyAllExpr:
		return colNamesFromExpr(v.Left)
	case *SubqueryExpr:
		return nil
	case *Literal:
		return nil
	}
	return nil
}
