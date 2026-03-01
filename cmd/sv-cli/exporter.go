package main

import (
	"encoding/csv"
	"fmt"
	"os"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

type Exporter struct {
	db *sqlvibe.Database
}

func NewExporter(db *sqlvibe.Database) *Exporter {
	return &Exporter{db: db}
}

func (e *Exporter) ExportCSV(filename, table string) error {
	rows, err := e.db.Query("SELECT * FROM " + table)
	if err != nil {
		return err
	}

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if len(rows.Columns) > 0 {
		writer.Write(rows.Columns)
	}

	for _, row := range rows.Data {
		rowStr := make([]string, len(row))
		for i, v := range row {
			if v == nil {
				rowStr[i] = ""
			} else {
				rowStr[i] = fmt.Sprintf("%v", v)
			}
		}
		writer.Write(rowStr)
	}

	return nil
}

func (e *Exporter) ExportJSON(filename, table string) error {
	rows, err := e.db.Query("SELECT * FROM " + table)
	if err != nil {
		return err
	}

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	fmt.Fprintln(file, "[")

	for i, row := range rows.Data {
		fmt.Fprint(file, "  {")
		for j, col := range rows.Columns {
			if j > 0 {
				fmt.Fprint(file, ", ")
			}
			fmt.Fprintf(file, `"%s": "%v"`, col, row[j])
		}
		fmt.Fprint(file, "}")
		if i < len(rows.Data)-1 {
			fmt.Fprintln(file, ",")
		} else {
			fmt.Fprintln(file)
		}
	}

	fmt.Fprintln(file, "]")

	return nil
}
