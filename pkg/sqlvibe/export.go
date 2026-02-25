package sqlvibe

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
)

// CSVExportOptions controls how ExportCSV behaves.
type CSVExportOptions struct {
	WriteHeader bool   // write column names as first row (default: true)
	Comma       rune   // field delimiter (default: ',')
	NullString  string // representation for NULL values (default: "")
}

// ExportCSV executes sql and writes the result as CSV to w.
func (db *Database) ExportCSV(w io.Writer, sql string, opts CSVExportOptions) error {
	if opts.Comma == 0 {
		opts.Comma = ','
	}

	rows, err := db.Query(sql)
	if err != nil {
		return fmt.Errorf("ExportCSV: query: %w", err)
	}

	cw := csv.NewWriter(w)
	cw.Comma = opts.Comma

	if opts.WriteHeader {
		if err2 := cw.Write(rows.Columns); err2 != nil {
			return fmt.Errorf("ExportCSV: writing header: %w", err2)
		}
	}

	for _, row := range rows.Data {
		record := make([]string, len(row))
		for i, v := range row {
			if v == nil {
				record[i] = opts.NullString
			} else {
				record[i] = valueToString(v)
			}
		}
		if err2 := cw.Write(record); err2 != nil {
			return fmt.Errorf("ExportCSV: writing row: %w", err2)
		}
	}

	cw.Flush()
	return cw.Error()
}

// ExportJSON executes sql and writes the result as a JSON array of objects to w.
// NULL values are written as JSON null literals.
func (db *Database) ExportJSON(w io.Writer, sql string) error {
	rows, err := db.Query(sql)
	if err != nil {
		return fmt.Errorf("ExportJSON: query: %w", err)
	}

	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)

	objects := make([]map[string]interface{}, 0, len(rows.Data))
	for _, row := range rows.Data {
		obj := make(map[string]interface{}, len(rows.Columns))
		for i, col := range rows.Columns {
			if i < len(row) {
				obj[col] = row[i]
			} else {
				obj[col] = nil
			}
		}
		objects = append(objects, obj)
	}

	return enc.Encode(objects)
}

// valueToString converts a query result value to its string representation for CSV output.
func valueToString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case int64:
		return strconv.FormatInt(val, 10)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		if val {
			return "1"
		}
		return "0"
	default:
		return fmt.Sprintf("%v", v)
	}
}
