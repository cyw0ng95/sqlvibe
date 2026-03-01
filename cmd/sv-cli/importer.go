package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

type Importer struct {
	db *sqlvibe.Database
}

func NewImporter(db *sqlvibe.Database) *Importer {
	return &Importer{db: db}
}

func (i *Importer) ImportCSV(filename, table string) (int, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return 0, err
	}

	if len(records) == 0 {
		return 0, fmt.Errorf("empty CSV file")
	}

	headers := records[0]
	rows := records[1:]
	if len(rows) == 0 {
		return 0, fmt.Errorf("no data rows in CSV")
	}

	colList := make([]string, len(headers))
	placeholders := make([]string, len(headers))
	for j := range headers {
		colList[j] = fmt.Sprintf(`"%s" TEXT`, headers[j])
		placeholders[j] = "?"
	}

	createSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", table, strings.Join(colList, ", "))
	if _, err := i.db.Exec(createSQL); err != nil {
		return 0, err
	}

	count := 0
	for _, row := range rows {
		insertSQL := fmt.Sprintf("INSERT INTO %s VALUES (%s)", table, strings.Join(placeholders, ", "))
		stmt, err := i.db.Prepare(insertSQL)
		if err != nil {
			continue
		}
		vals := make([]interface{}, len(row))
		for k, v := range row {
			vals[k] = v
		}
		_, err = stmt.Exec(vals...)
		if err != nil {
			continue
		}
		count++
	}

	return count, nil
}
