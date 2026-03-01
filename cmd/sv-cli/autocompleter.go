package main

import (
	"strings"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

type AutoCompleter struct {
	tables   []string
	columns  map[string][]string
	keywords []string
}

func NewAutoCompleter() *AutoCompleter {
	return &AutoCompleter{
		tables:  []string{},
		columns: make(map[string][]string),
		keywords: []string{
			"SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE",
			"CREATE", "TABLE", "INDEX", "DROP", "ALTER", "ADD",
			"JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "ON",
			"AND", "OR", "NOT", "IN", "LIKE", "BETWEEN",
			"ORDER", "BY", "GROUP", "HAVING", "LIMIT", "OFFSET",
			"NULL", "IS", "AS", "DISTINCT", "UNION", "ALL",
			"CASE", "WHEN", "THEN", "ELSE", "END",
			"PRIMARY", "KEY", "FOREIGN", "REFERENCES", "UNIQUE",
			"CHECK", "DEFAULT", "NOT", "NULL", "AUTOINCREMENT",
		},
	}
}

func (a *AutoCompleter) UpdateSchema(db *sqlvibe.Database) {
	tables, err := db.GetTables()
	if err != nil {
		return
	}
	a.tables = nil
	a.columns = make(map[string][]string)
	for _, t := range tables {
		a.tables = append(a.tables, t.Name)
		rows, err := db.Query("PRAGMA table_info(" + t.Name + ")")
		if err != nil {
			continue
		}
		var cols []string
		for _, row := range rows.Data {
			if len(row) > 1 {
				cols = append(cols, row[1].(string))
			}
		}
		a.columns[t.Name] = cols
	}
}

func (a *AutoCompleter) Complete(prefix string) []string {
	var suggestions []string
	upperPrefix := strings.ToUpper(prefix)

	for _, kw := range a.keywords {
		if strings.HasPrefix(kw, upperPrefix) {
			suggestions = append(suggestions, kw)
		}
	}

	for _, table := range a.tables {
		if strings.HasPrefix(strings.ToUpper(table), upperPrefix) {
			suggestions = append(suggestions, table)
		}
	}

	return suggestions
}
