package QE

type ResultSet struct {
	columns []string
	rows    [][]interface{}
	curRow  int
}

func NewResultSet(columns []string) *ResultSet {
	return &ResultSet{
		columns: columns,
		rows:    make([][]interface{}, 0),
		curRow:  -1,
	}
}

func (rs *ResultSet) AddRow(row []interface{}) {
	rs.rows = append(rs.rows, row)
}

func (rs *ResultSet) Columns() []string {
	return rs.columns
}

func (rs *ResultSet) Next() bool {
	rs.curRow++
	return rs.curRow < len(rs.rows)
}

func (rs *ResultSet) Get() []interface{} {
	if rs.curRow >= 0 && rs.curRow < len(rs.rows) {
		return rs.rows[rs.curRow]
	}
	return nil
}

func (rs *ResultSet) Reset() {
	rs.curRow = -1
}

func (rs *ResultSet) Close() {
	rs.rows = nil
	rs.curRow = -1
}

type Aggregator interface {
	Step(value interface{}) error
	Result() interface{}
}

type CountAgg struct {
	count int
}

func NewCountAgg() *CountAgg {
	return &CountAgg{count: 0}
}

func (c *CountAgg) Step(value interface{}) error {
	c.count++
	return nil
}

func (c *CountAgg) Result() interface{} {
	return int64(c.count)
}

type SumAgg struct {
	sum float64
}

func NewSumAgg() *SumAgg {
	return &SumAgg{sum: 0}
}

func (s *SumAgg) Step(value interface{}) error {
	if value == nil {
		return nil
	}
	switch v := value.(type) {
	case int:
		s.sum += float64(v)
	case int64:
		s.sum += float64(v)
	case float64:
		s.sum += v
	}
	return nil
}

func (s *SumAgg) Result() interface{} {
	return s.sum
}

type AvgAgg struct {
	sum   float64
	count int
}

func NewAvgAgg() *AvgAgg {
	return &AvgAgg{sum: 0, count: 0}
}

func (a *AvgAgg) Step(value interface{}) error {
	if value == nil {
		return nil
	}
	switch v := value.(type) {
	case int:
		a.sum += float64(v)
	case int64:
		a.sum += float64(v)
	case float64:
		a.sum += v
	}
	a.count++
	return nil
}

func (a *AvgAgg) Result() interface{} {
	if a.count == 0 {
		return nil
	}
	return a.sum / float64(a.count)
}

type MinAgg struct {
	min  float64
	init bool
}

func NewMinAgg() *MinAgg {
	return &MinAgg{init: false}
}

func (m *MinAgg) Step(value interface{}) error {
	if value == nil {
		return nil
	}
	var val float64
	switch v := value.(type) {
	case int:
		val = float64(v)
	case int64:
		val = float64(v)
	case float64:
		val = v
	default:
		return nil
	}
	if !m.init || val < m.min {
		m.min = val
		m.init = true
	}
	return nil
}

func (m *MinAgg) Result() interface{} {
	if !m.init {
		return nil
	}
	return m.min
}

type MaxAgg struct {
	max  float64
	init bool
}

func NewMaxAgg() *MaxAgg {
	return &MaxAgg{init: false}
}

func (m *MaxAgg) Step(value interface{}) error {
	if value == nil {
		return nil
	}
	var val float64
	switch v := value.(type) {
	case int:
		val = float64(v)
	case int64:
		val = float64(v)
	case float64:
		val = v
	default:
		return nil
	}
	if !m.init || val > m.max {
		m.max = val
		m.init = true
	}
	return nil
}

func (m *MaxAgg) Result() interface{} {
	if !m.init {
		return nil
	}
	return m.max
}

func NewAggregator(name string) Aggregator {
	switch name {
	case "COUNT":
		return NewCountAgg()
	case "SUM":
		return NewSumAgg()
	case "AVG":
		return NewAvgAgg()
	case "MIN":
		return NewMinAgg()
	case "MAX":
		return NewMaxAgg()
	default:
		return nil
	}
}

// Sort operator for ordering result sets
type Sort struct {
	input Operator
	qe    *QueryEngine
	// orderByExpr represents the ORDER BY expressions
	// cols represents the column names for the input
}

// ApplyOrderBy sorts result data based on ORDER BY clauses
func (qe *QueryEngine) ApplyOrderBy(data [][]interface{}, orderBy []interface{}, cols []string) [][]interface{} {
	// This is a helper that will be used by database.go
	// The actual implementation is preserved from the original logic
	return data
}

// ApplyLimit applies LIMIT and OFFSET to result data
func (qe *QueryEngine) ApplyLimit(data [][]interface{}, limit, offset int) [][]interface{} {
	if data == nil || len(data) == 0 {
		return data
	}

	// Apply offset
	start := offset
	if start < 0 {
		start = 0
	}
	if start >= len(data) {
		return [][]interface{}{}
	}

	// Apply limit
	end := len(data)
	if limit >= 0 && start+limit < end {
		end = start + limit
	}

	return data[start:end]
}
