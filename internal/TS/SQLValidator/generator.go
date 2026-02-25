package SQLValidator

import (
	"fmt"
	"strings"
)

// Generator produces SQL SELECT statements that reference the TPC-C schema.
// All generated queries are read-only; data integrity is maintained by the
// setup phase in Validator.
type Generator struct {
	lcg *LCG
}

// NewGenerator creates a Generator that uses the given LCG for randomness.
func NewGenerator(lcg *LCG) *Generator {
	return &Generator{lcg: lcg}
}

// Next returns the next randomly generated SQL statement.
// The statement is guaranteed to be syntactically valid for SQLite/sqlvibe.
func (g *Generator) Next() string {
	// Weighted statement type selection (weights sum to 100).
	//  0-20  → simple SELECT (single table, optional WHERE, optional LIMIT)
	// 25-34  → SELECT with ORDER BY … LIMIT
	// 35-42  → SELECT COUNT / aggregate
	// 43-50  → SELECT with GROUP BY
	// 51-58  → DISTINCT
	// 59-66  → UNION / UNION ALL
	// 67-74  → two-table INNER JOIN
	// 75-80  → two-table LEFT JOIN
	// 81-85  → SELECT with IS NULL / IS NOT NULL predicate
	// 86-90  → SELECT with BETWEEN predicate
	// 91-94  → Subquery (IN/EXISTS)
	// 95-97  → HAVING
	// 98-99  → CASE WHEN
	w := g.lcg.Intn(100)
	switch {
	case w < 25:
		return g.genSimpleSelect()
	case w < 35:
		return g.genOrderByLimit()
	case w < 43:
		return g.genAggregate()
	case w < 51:
		return g.genGroupBy()
	case w < 59:
		return g.genDistinct()
	case w < 67:
		return g.genUnion()
	case w < 75:
		return g.genInnerJoin()
	case w < 81:
		return g.genLeftJoin()
	case w < 86:
		return g.genNullPredicate()
	case w < 91:
		return g.genBetween()
	case w < 95:
		return g.genSubquery()
	case w < 98:
		return g.genHaving()
	default:
		return g.genCaseWhen()
	}
}

// randomTable returns a randomly chosen table from tpccTables.
func (g *Generator) randomTable() *tableMeta {
	return &tpccTables[g.lcg.Intn(len(tpccTables))]
}

// randomCols returns a random non-empty subset of column names.
// It always picks at least 1 and at most min(3, len(cols)) columns.
func (g *Generator) randomCols(tm *tableMeta) []string {
	all := tm.allColNames()
	n := g.lcg.Intn(3) + 1 // 1..3
	if n > len(all) {
		n = len(all)
	}
	// Shuffle a copy
	chosen := make([]string, len(all))
	copy(chosen, all)
	for i := len(chosen) - 1; i > 0; i-- {
		j := g.lcg.Intn(i + 1)
		chosen[i], chosen[j] = chosen[j], chosen[i]
	}
	return chosen[:n]
}

// intLiterals returns a small set of interesting integer values.
var intLiterals = []string{"0", "1", "2", "3", "4", "5"}

// realLiterals returns a small set of interesting real values.
var realLiterals = []string{"0.0", "0.05", "0.10", "50000.0", "100.0"}

// randomIntLit returns a random integer literal.
func (g *Generator) randomIntLit() string {
	return g.lcg.Choice(intLiterals)
}

// randomRealLit returns a random real literal.
func (g *Generator) randomRealLit() string {
	return g.lcg.Choice(realLiterals)
}

// simpleWhere generates a simple equality/comparison WHERE clause for a
// NOT NULL INTEGER column, avoiding NULL comparison issues.
// Returns "" (no WHERE) with probability 1/3.
func (g *Generator) simpleWhere(tm *tableMeta) string {
	if g.lcg.Intn(3) == 0 {
		return ""
	}
	// Pick a NOT NULL integer column for the predicate.
	cols := tm.nonNullIntCols()
	if len(cols) == 0 {
		return ""
	}
	col := g.lcg.Choice(cols)
	ops := []string{"=", ">", "<", ">=", "<="}
	op := g.lcg.Choice(ops)
	val := g.randomIntLit()
	return fmt.Sprintf(" WHERE %s %s %s", col, op, val)
}

// pkOrderBy returns a deterministic ORDER BY clause using all PK columns of tm.
// When using LIMIT, always include the full PK so ties are broken the same way
// in SQLite and sqlvibe.
func (g *Generator) pkOrderBy(tm *tableMeta, dir string) string {
	if len(tm.pkCols) == 0 {
		return ""
	}
	parts := make([]string, len(tm.pkCols))
	for i, c := range tm.pkCols {
		parts[i] = c + " " + dir
	}
	return " ORDER BY " + strings.Join(parts, ", ")
}

// genSimpleSelect generates: SELECT <cols> FROM <table> [WHERE ...] [ORDER BY pk] [LIMIT n]
func (g *Generator) genSimpleSelect() string {
	tm := g.randomTable()
	cols := g.randomCols(tm)
	where := g.simpleWhere(tm)
	limit := ""
	orderBy := ""
	if g.lcg.Intn(2) == 0 {
		// Always add ORDER BY PK when using LIMIT to ensure deterministic results.
		orderBy = g.pkOrderBy(tm, "ASC")
		limit = fmt.Sprintf(" LIMIT %d", g.lcg.Intn(10)+1)
	}
	return fmt.Sprintf("SELECT %s FROM %s%s%s%s",
		strings.Join(cols, ", "), tm.name, where, orderBy, limit)
}

// genOrderByLimit generates: SELECT <col> FROM <table> ORDER BY <col> [, pk cols] [ASC|DESC] LIMIT n
// The full primary key is appended to break ties and ensure deterministic output.
func (g *Generator) genOrderByLimit() string {
	tm := g.randomTable()
	cols := g.randomCols(tm)
	// Pick an INTEGER or REAL column for ORDER BY to get stable ordering.
	var orderCol string
	for _, c := range tm.columns {
		if c.colType == "INTEGER" || c.colType == "REAL" {
			orderCol = c.name
			break
		}
	}
	if orderCol == "" {
		orderCol = tm.columns[0].name
	}
	dirs := []string{"ASC", "DESC"}
	dir := g.lcg.Choice(dirs)
	limit := g.lcg.Intn(10) + 1
	// Build a fully deterministic ORDER BY: chosen col + all PK cols.
	orderCols := []string{orderCol + " " + dir}
	for _, pk := range tm.pkCols {
		if pk != orderCol {
			orderCols = append(orderCols, pk+" "+dir)
		}
	}
	return fmt.Sprintf("SELECT %s FROM %s ORDER BY %s LIMIT %d",
		strings.Join(cols, ", "), tm.name, strings.Join(orderCols, ", "), limit)
}

// genAggregate generates: SELECT COUNT(*)|SUM|AVG|MIN|MAX FROM <table>
func (g *Generator) genAggregate() string {
	tm := g.randomTable()
	// Pick a NOT NULL INTEGER or REAL column for numeric aggregates.
	var numCols []string
	for _, c := range tm.columns {
		if (c.colType == "INTEGER" || c.colType == "REAL") && c.notNull {
			numCols = append(numCols, c.name)
		}
	}
	// COUNT(*) is always valid.
	funcs := []string{"COUNT(*)"}
	if len(numCols) > 0 {
		col := g.lcg.Choice(numCols)
		funcs = append(funcs, fmt.Sprintf("SUM(%s)", col))
		funcs = append(funcs, fmt.Sprintf("MIN(%s)", col))
		funcs = append(funcs, fmt.Sprintf("MAX(%s)", col))
	}
	expr := g.lcg.Choice(funcs)
	where := g.simpleWhere(tm)
	return fmt.Sprintf("SELECT %s FROM %s%s", expr, tm.name, where)
}

// genGroupBy generates: SELECT <col>, COUNT(*) FROM <table> GROUP BY <col> ORDER BY <col>
func (g *Generator) genGroupBy() string {
	tm := g.randomTable()
	// Use an INTEGER column with small cardinality for GROUP BY to keep results stable.
	var intCols []string
	for _, c := range tm.columns {
		if c.colType == "INTEGER" && c.notNull {
			intCols = append(intCols, c.name)
		}
	}
	if len(intCols) == 0 {
		// Fall back to a simple aggregate.
		return g.genAggregate()
	}
	groupCol := g.lcg.Choice(intCols)
	// ORDER BY groupCol to make output deterministic.
	return fmt.Sprintf("SELECT %s, COUNT(*) FROM %s GROUP BY %s ORDER BY %s ASC",
		groupCol, tm.name, groupCol, groupCol)
}

// genInnerJoin generates a two-table INNER JOIN with a deterministic ORDER BY.
// Pre-defined join pairs use PK columns from both tables in the ORDER BY so
// LIMIT always returns the same rows in SQLite and sqlvibe.
func (g *Generator) genInnerJoin() string {
	// Pre-defined join pairs that are meaningful in TPC-C.
	type joinPair struct {
		t1, t2, on, sel, orderBy string
	}
	pairs := []joinPair{
		{
			"warehouse", "district",
			"warehouse.w_id = district.d_w_id",
			"warehouse.w_id, warehouse.w_name, district.d_id, district.d_name",
			"warehouse.w_id ASC, district.d_id ASC",
		},
		{
			"item", "stock",
			"item.i_id = stock.s_i_id",
			"item.i_id, item.i_name, stock.s_w_id, stock.s_quantity",
			"item.i_id ASC, stock.s_w_id ASC",
		},
		{
			"orders", "customer",
			"orders.o_c_id = customer.c_id AND orders.o_d_id = customer.c_d_id AND orders.o_w_id = customer.c_w_id",
			"orders.o_id, customer.c_first, customer.c_last",
			"orders.o_id ASC, orders.o_d_id ASC, orders.o_w_id ASC",
		},
		{
			"warehouse", "stock",
			"warehouse.w_id = stock.s_w_id",
			"warehouse.w_id, stock.s_i_id, stock.s_quantity",
			"warehouse.w_id ASC, stock.s_i_id ASC",
		},
	}
	p := pairs[g.lcg.Intn(len(pairs))]
	limit := g.lcg.Intn(10) + 1
	return fmt.Sprintf("SELECT %s FROM %s INNER JOIN %s ON %s ORDER BY %s LIMIT %d",
		p.sel, p.t1, p.t2, p.on, p.orderBy, limit)
}

// genLeftJoin generates a LEFT JOIN variant with deterministic ORDER BY.
func (g *Generator) genLeftJoin() string {
	// Use item-stock LEFT JOIN (there can be items with no stock).
	limit := g.lcg.Intn(10) + 1
	col := g.lcg.Choice([]string{"i_id", "i_name", "i_price"})
	// ORDER BY item.i_id, stock.s_w_id ensures deterministic ordering even for NULLs.
	return fmt.Sprintf("SELECT item.i_id, item.%s, stock.s_quantity FROM item LEFT JOIN stock ON item.i_id = stock.s_i_id ORDER BY item.i_id ASC, stock.s_w_id ASC LIMIT %d",
		col, limit)
}

// genNullPredicate generates: SELECT ... WHERE <nullable_col> IS [NOT] NULL ORDER BY pk LIMIT 10
func (g *Generator) genNullPredicate() string {
	// Use tables/columns where nullable values exist in our seed data.
	type nullPred struct {
		table, col, sel, orderBy string
	}
	preds := []nullPred{
		{"orders", "o_carrier_id", "o_id, o_d_id, o_w_id, o_carrier_id",
			"o_id ASC, o_d_id ASC, o_w_id ASC"},
		{"order_line", "ol_delivery_d", "ol_o_id, ol_d_id, ol_delivery_d",
			"ol_o_id ASC, ol_d_id ASC, ol_w_id ASC, ol_number ASC"},
		{"stock", "s_data", "s_i_id, s_w_id, s_data",
			"s_i_id ASC, s_w_id ASC"},
		{"customer", "c_data", "c_id, c_d_id, c_data",
			"c_id ASC, c_d_id ASC, c_w_id ASC"},
		{"item", "i_data", "i_id, i_data",
			"i_id ASC"},
	}
	p := preds[g.lcg.Intn(len(preds))]
	notOpt := ""
	if g.lcg.Intn(2) == 0 {
		notOpt = "NOT "
	}
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s IS %sNULL ORDER BY %s LIMIT 10",
		p.sel, p.table, p.col, notOpt, p.orderBy)
}

// genBetween generates: SELECT ... WHERE <int_col> BETWEEN low AND high ORDER BY pk LIMIT 10
func (g *Generator) genBetween() string {
	tm := g.randomTable()
	intCols := tm.nonNullIntCols()
	if len(intCols) == 0 {
		return g.genSimpleSelect()
	}
	col := g.lcg.Choice(intCols)
	lo := g.lcg.Intn(5)
	hi := lo + g.lcg.Intn(5) + 1
	cols := g.randomCols(tm)
	// Use full PK in ORDER BY to ensure deterministic results.
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s BETWEEN %d AND %d%s LIMIT 10",
		strings.Join(cols, ", "), tm.name, col, lo, hi, g.pkOrderBy(tm, "ASC"))
}

// genDistinct generates: SELECT DISTINCT <cols> FROM <table> [WHERE ...] [ORDER BY ...] LIMIT n
func (g *Generator) genDistinct() string {
	tm := g.randomTable()
	cols := g.randomCols(tm)
	where := g.simpleWhere(tm)
	orderBy := g.pkOrderBy(tm, "ASC")
	limit := g.lcg.Intn(10) + 1
	return fmt.Sprintf("SELECT DISTINCT %s FROM %s%s%s LIMIT %d",
		strings.Join(cols, ", "), tm.name, where, orderBy, limit)
}

// genUnion generates: SELECT ... FROM <table1> ... UNION [ALL] SELECT ... FROM <table2> ...
func (g *Generator) genUnion() string {
	tm1 := g.randomTable()
	tm2 := g.randomTable()
	cols1 := g.randomCols(tm1)
	cols2 := g.randomCols(tm2)

	all := g.lcg.Choice([]string{"", "ALL"})
	limit := g.lcg.Intn(10) + 1

	// Use same number of columns for UNION
	n := len(cols1)
	if n > len(cols2) {
		n = len(cols2)
	}
	cols1 = cols1[:n]
	cols2 = cols2[:n]

	return fmt.Sprintf("SELECT %s FROM %s%s UNION %s SELECT %s FROM %s ORDER BY 1 LIMIT %d",
		strings.Join(cols1, ", "), tm1.name, g.pkOrderBy(tm1, "ASC"),
		all,
		strings.Join(cols2, ", "), tm2.name, limit)
}

// genSubquery generates: SELECT ... WHERE <col> IN (SELECT ... FROM <table>) or EXISTS
func (g *Generator) genSubquery() string {
	tm1 := g.randomTable()
	tm2 := g.randomTable()

	intCols := tm1.nonNullIntCols()
	if len(intCols) == 0 {
		return g.genSimpleSelect()
	}
	col := g.lcg.Choice(intCols)

	isExists := g.lcg.Intn(2) == 0
	if isExists {
		return fmt.Sprintf("SELECT %s FROM %s WHERE EXISTS (SELECT 1 FROM %s WHERE %s = %s.%s) LIMIT 10",
			tm1.columns[0].name, tm1.name, tm2.name, tm1.name, tm2.name, intCols[0])
	}

	return fmt.Sprintf("SELECT %s FROM %s WHERE %s IN (SELECT %s FROM %s) LIMIT 10",
		tm1.columns[0].name, tm1.name, col, intCols[0], tm2.name)
}

// genHaving generates: SELECT <col>, COUNT(*) FROM <table> GROUP BY <col> HAVING COUNT(*) > n
func (g *Generator) genHaving() string {
	tm := g.randomTable()
	intCols := tm.nonNullIntCols()
	if len(intCols) == 0 {
		return g.genAggregate()
	}
	groupCol := g.lcg.Choice(intCols)
	threshold := g.lcg.Intn(3) + 1
	return fmt.Sprintf("SELECT %s, COUNT(*) FROM %s GROUP BY %s HAVING COUNT(*) > %d ORDER BY %s ASC LIMIT 10",
		groupCol, tm.name, groupCol, threshold, groupCol)
}

// genCaseWhen generates: SELECT CASE WHEN <cond> THEN ... ELSE ... END FROM <table>
func (g *Generator) genCaseWhen() string {
	tm := g.randomTable()
	intCols := tm.nonNullIntCols()
	if len(intCols) == 0 {
		return g.genSimpleSelect()
	}
	col := g.lcg.Choice(intCols)
	selCols := g.randomCols(tm)

	return fmt.Sprintf("SELECT %s, CASE WHEN %s > 2 THEN 'high' WHEN %s > 0 THEN 'low' ELSE 'zero' END AS label FROM %s%s LIMIT 10",
		strings.Join(selCols, ", "), col, col, tm.name, g.pkOrderBy(tm, "ASC"))
}
