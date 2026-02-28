package sqlvibe

import (
	"fmt"
	"strconv"

	DS "github.com/cyw0ng95/sqlvibe/internal/DS"
	IS "github.com/cyw0ng95/sqlvibe/internal/IS"
)

func init() {
	IS.RegisterVTabModule("series", &seriesModule{})
}

// seriesModule implements DS.VTabModule for the series virtual table.
// Usage: SELECT * FROM series(start, stop[, step]) or
//
//	CREATE VIRTUAL TABLE t USING series(start, stop[, step])
//
// Produces a single column "value INTEGER" with integers in [start, stop] by step.
type seriesModule struct {
	DS.TableModule
}

func (m *seriesModule) Create(args []string) (DS.VTab, error) {
	return parseSeriesArgs(args)
}

func (m *seriesModule) Connect(args []string) (DS.VTab, error) {
	return parseSeriesArgs(args)
}

func parseSeriesArgs(args []string) (*seriesVTab, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("series: requires at least 2 arguments (start, stop)")
	}
	start, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("series: invalid start %q: %v", args[0], err)
	}
	stop, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("series: invalid stop %q: %v", args[1], err)
	}
	vt := &seriesVTab{start: start, stop: stop, step: 1}
	if len(args) >= 3 {
		v, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("series: invalid step %q: %v", args[2], err)
		}
		if v == 0 {
			return nil, fmt.Errorf("series: step cannot be zero")
		}
		vt.step = v
	}
	return vt, nil
}

type seriesVTab struct {
	start, stop, step int64
}

func (vt *seriesVTab) BestIndex(info *DS.IndexInfo) error { return nil }

func (vt *seriesVTab) Open() (DS.VTabCursor, error) {
	return &seriesCursor{
		start:   vt.start,
		stop:    vt.stop,
		step:    vt.step,
		current: vt.start,
	}, nil
}

func (vt *seriesVTab) Columns() []string { return []string{"value"} }

func (vt *seriesVTab) Disconnect() error { return nil }

func (vt *seriesVTab) Destroy() error { return nil }

type seriesCursor struct {
	start, stop, step int64
	current           int64
}

func (c *seriesCursor) Filter(idxNum int, idxStr string, args []interface{}) error {
	c.current = c.start
	return nil
}

func (c *seriesCursor) Next() error {
	c.current += c.step
	return nil
}

func (c *seriesCursor) Column(col int) (interface{}, error) {
	if col != 0 {
		return nil, fmt.Errorf("series: column %d out of range", col)
	}
	return c.current, nil
}

func (c *seriesCursor) RowID() (int64, error) {
	return (c.current - c.start) / c.step, nil
}

func (c *seriesCursor) Eof() bool {
	if c.step > 0 {
		return c.current > c.stop
	}
	return c.current < c.stop
}

func (c *seriesCursor) Close() error { return nil }
