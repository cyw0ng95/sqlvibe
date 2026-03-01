package main

import (
	"fmt"
	"strings"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

type Formatter struct {
	mode         string
	showHeaders  bool
	nullValue    string
	columnWidths []int
}

func NewFormatter() *Formatter {
	return &Formatter{
		mode:        "table",
		showHeaders: true,
		nullValue:   "",
	}
}

func (f *Formatter) SetMode(mode string) {
	f.mode = mode
}

func (f *Formatter) SetShowHeaders(show bool) {
	f.showHeaders = show
}

func (f *Formatter) SetNullValue(value string) {
	f.nullValue = value
}

func (f *Formatter) SetColumnWidths(widths []int) {
	f.columnWidths = widths
}

func (f *Formatter) Format(rows *sqlvibe.Rows) string {
	if rows == nil || len(rows.Columns) == 0 {
		return ""
	}

	switch f.mode {
	case "csv":
		return f.formatCSV(rows)
	case "json":
		return f.formatJSON(rows)
	case "list":
		return f.formatList(rows)
	default:
		return f.formatTable(rows)
	}
}

func (f *Formatter) formatTable(rows *sqlvibe.Rows) string {
	var sb strings.Builder
	cols := rows.Columns
	widths := make([]int, len(cols))
	for i, c := range cols {
		widths[i] = len(c)
	}

	for _, row := range rows.Data {
		for i, v := range row {
			s := f.formatValue(v)
			if len(s) > widths[i] {
				widths[i] = len(s)
			}
		}
	}

	if f.showHeaders {
		sb.WriteString(strings.Repeat("-", sum(widths)+len(cols)*3+1) + "\n")
		header := "|"
		for i, c := range cols {
			header += fmt.Sprintf(" %-*s |", widths[i], c)
		}
		sb.WriteString(header + "\n")
		sb.WriteString(strings.Repeat("-", sum(widths)+len(cols)*3+1) + "\n")
	}

	for _, row := range rows.Data {
		rowStr := "|"
		for i, v := range row {
			rowStr += fmt.Sprintf(" %-*s |", widths[i], f.formatValue(v))
		}
		sb.WriteString(rowStr + "\n")
	}

	return sb.String()
}

func (f *Formatter) formatCSV(rows *sqlvibe.Rows) string {
	var sb strings.Builder
	if f.showHeaders {
		sb.WriteString(strings.Join(rows.Columns, ",") + "\n")
	}
	for _, row := range rows.Data {
		var vals []string
		for _, v := range row {
			vals = append(vals, f.formatValue(v))
		}
		sb.WriteString(strings.Join(vals, ",") + "\n")
	}
	return sb.String()
}

func (f *Formatter) formatJSON(rows *sqlvibe.Rows) string {
	var sb strings.Builder
	sb.WriteString("[\n")
	for i, row := range rows.Data {
		sb.WriteString("  {")
		var pairs []string
		for j, col := range rows.Columns {
			pairs = append(pairs, fmt.Sprintf(`"%s": "%s"`, col, f.formatValue(row[j])))
		}
		sb.WriteString(strings.Join(pairs, ", "))
		sb.WriteString("}")
		if i < len(rows.Data)-1 {
			sb.WriteString(",")
		}
		sb.WriteString("\n")
	}
	sb.WriteString("]\n")
	return sb.String()
}

func (f *Formatter) formatList(rows *sqlvibe.Rows) string {
	var sb strings.Builder
	for _, row := range rows.Data {
		for i, col := range rows.Columns {
			sb.WriteString(fmt.Sprintf("%s = %s\n", col, f.formatValue(row[i])))
		}
		sb.WriteString("\n")
	}
	return sb.String()
}

func (f *Formatter) formatValue(v interface{}) string {
	if v == nil {
		if f.nullValue != "" {
			return f.nullValue
		}
		return "NULL"
	}
	return fmt.Sprintf("%v", v)
}

func sum(nums []int) int {
	s := 0
	for _, n := range nums {
		s += n
	}
	return s
}

type OutputMode string

const (
	OutputCSV   OutputMode = "csv"
	OutputTable OutputMode = "table"
	OutputList  OutputMode = "list"
	OutputJSON  OutputMode = "json"
)
