package sqlvibe

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

// DumpOptions controls how Dump behaves.
type DumpOptions struct {
	DataOnly   bool // only output INSERT statements, no schema
	SchemaOnly bool // only output CREATE statements, no data
	UseInserts bool // output data as INSERT statements (always true for Dump)
}

// Dump writes an SQL dump of the entire database to w.
// By default, both schema and data are dumped as SQL statements.
func (db *Database) Dump(w io.Writer, opts DumpOptions) error {
	if opts.DataOnly && opts.SchemaOnly {
		return fmt.Errorf("Dump: DataOnly and SchemaOnly cannot both be set")
	}

	// Collect and sort table names for deterministic output
	tableNames := make([]string, 0, len(db.tables))
	for name := range db.tables {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)

	if !opts.DataOnly {
		// Emit CREATE TABLE statements
		for _, tableName := range tableNames {
			sql := db.reconstructCreateTableSQL(tableName)
			if _, err := fmt.Fprintf(w, "%s;\n", sql); err != nil {
				return err
			}
		}

		// Emit CREATE VIEW statements
		viewNames := make([]string, 0, len(db.views))
		for name := range db.views {
			viewNames = append(viewNames, name)
		}
		sort.Strings(viewNames)
		for _, viewName := range viewNames {
			if _, err := fmt.Fprintf(w, "CREATE VIEW %s AS %s;\n", viewName, db.views[viewName]); err != nil {
				return err
			}
		}

		// Emit CREATE INDEX statements
		idxNames := make([]string, 0, len(db.indexes))
		for name := range db.indexes {
			idxNames = append(idxNames, name)
		}
		sort.Strings(idxNames)
		for _, idxName := range idxNames {
			idx := db.indexes[idxName]
			unique := ""
			if idx.Unique {
				unique = "UNIQUE "
			}
			if _, err := fmt.Fprintf(w, "CREATE %sINDEX %s ON %s (%s);\n",
				unique, idxName, idx.Table, strings.Join(idx.Columns, ", ")); err != nil {
				return err
			}
		}
	}

	if !opts.SchemaOnly {
		// Emit INSERT statements for each table
		for _, tableName := range tableNames {
			rows := db.data[tableName]
			if len(rows) == 0 {
				continue
			}
			cols := db.columnOrder[tableName]
			if len(cols) == 0 {
				continue
			}

			quotedCols := make([]string, len(cols))
			for i, c := range cols {
				quotedCols[i] = fmt.Sprintf(`"%s"`, c)
			}
			colList := strings.Join(quotedCols, ", ")

			for _, row := range rows {
				vals := make([]string, len(cols))
				for i, c := range cols {
					vals[i] = dumpSQLLiteral(row[c])
				}
				if _, err := fmt.Fprintf(w, "INSERT INTO %q (%s) VALUES (%s);\n",
					tableName, colList, strings.Join(vals, ", ")); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// DumpDataOnly writes only INSERT statements for all tables to w.
func (db *Database) DumpDataOnly(w io.Writer) error {
	return db.Dump(w, DumpOptions{DataOnly: true})
}

// DumpSchemaOnly writes only CREATE TABLE/VIEW/INDEX statements to w.
func (db *Database) DumpSchemaOnly(w io.Writer) error {
	return db.Dump(w, DumpOptions{SchemaOnly: true})
}

// dumpSQLLiteral converts a Go value to an SQL literal string suitable for INSERT statements.
func dumpSQLLiteral(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case int64:
		return fmt.Sprintf("%d", val)
	case float64:
		return fmt.Sprintf("%g", val)
	case bool:
		if val {
			return "1"
		}
		return "0"
	case string:
		return "'" + strings.ReplaceAll(val, "'", "''") + "'"
	case []byte:
		return fmt.Sprintf("X'%X'", val)
	default:
		s := fmt.Sprintf("%v", v)
		return "'" + strings.ReplaceAll(s, "'", "''") + "'"
	}
}
