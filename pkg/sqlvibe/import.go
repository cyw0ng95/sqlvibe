package sqlvibe

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// CSVImportOptions controls how ImportCSV behaves.
type CSVImportOptions struct {
	HasHeader   bool   // first row contains column names (default: true)
	Comma       rune   // field delimiter (default: ',')
	CreateTable bool   // CREATE TABLE IF NOT EXISTS before importing (default: false)
	NullString  string // string value treated as NULL (default: "")
}

// ImportCSV reads CSV data from r and inserts rows into tableName.
// It returns the number of rows inserted, or an error.
func (db *Database) ImportCSV(tableName string, r io.Reader, opts CSVImportOptions) (int, error) {
	if opts.Comma == 0 {
		opts.Comma = ','
	}

	cr := csv.NewReader(r)
	cr.Comma = opts.Comma
	cr.TrimLeadingSpace = true

	// Read header row to get column names
	var cols []string
	if opts.HasHeader {
		header, err := cr.Read()
		if err != nil {
			if err == io.EOF {
				return 0, nil
			}
			return 0, fmt.Errorf("ImportCSV: reading header: %w", err)
		}
		cols = make([]string, len(header))
		for i, h := range header {
			cols[i] = strings.TrimSpace(h)
		}
	} else {
		// Read first data row to determine number of columns
		first, err := cr.Read()
		if err != nil {
			if err == io.EOF {
				return 0, nil
			}
			return 0, fmt.Errorf("ImportCSV: reading first row: %w", err)
		}
		cols = make([]string, len(first))
		for i := range first {
			cols[i] = fmt.Sprintf("c%d", i)
		}
		// Process the first row we already read
		if err2 := db.insertCSVRow(tableName, cols, first, opts.NullString); err2 != nil {
			return 0, err2
		}
		// Continue reading remaining rows
		count := 1
		for {
			record, rerr := cr.Read()
			if rerr == io.EOF {
				break
			}
			if rerr != nil {
				return count, fmt.Errorf("ImportCSV: reading row %d: %w", count+1, rerr)
			}
			if err2 := db.insertCSVRow(tableName, cols, record, opts.NullString); err2 != nil {
				return count, err2
			}
			count++
		}
		return count, nil
	}

	// Optionally create table based on inferred types from first data row
	if opts.CreateTable {
		colDefs := make([]string, len(cols))
		for i, c := range cols {
			colDefs[i] = fmt.Sprintf(`"%s" TEXT`, c)
		}
		createSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (%s)`, tableName, strings.Join(colDefs, ", "))
		if _, err := db.Exec(createSQL); err != nil {
			return 0, fmt.Errorf("ImportCSV: CREATE TABLE: %w", err)
		}
	}

	count := 0
	for {
		record, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return count, fmt.Errorf("ImportCSV: reading row %d: %w", count+1, err)
		}
		if err2 := db.insertCSVRow(tableName, cols, record, opts.NullString); err2 != nil {
			return count, err2
		}
		count++
	}
	return count, nil
}

// insertCSVRow inserts one CSV record into tableName using the normal INSERT path.
func (db *Database) insertCSVRow(tableName string, cols []string, record []string, nullStr string) error {
	if len(record) == 0 {
		return nil
	}
	quotedCols := make([]string, len(cols))
	for i, c := range cols {
		quotedCols[i] = fmt.Sprintf(`"%s"`, c)
	}
	placeholders := make([]string, len(record))
	for i, field := range record {
		if field == nullStr {
			placeholders[i] = "NULL"
		} else {
			placeholders[i] = inferCSVLiteral(field)
		}
	}
	sql := fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES (%s)`,
		tableName,
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "),
	)
	_, err := db.Exec(sql)
	return err
}

// inferCSVLiteral returns an SQL literal for a CSV field value.
// It tries int64, then float64, and falls back to a quoted string.
func inferCSVLiteral(s string) string {
	if _, err := strconv.ParseInt(s, 10, 64); err == nil {
		return s
	}
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		return s
	}
	// Escape single quotes and wrap in single quotes
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
