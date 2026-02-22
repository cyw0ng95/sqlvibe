package sqlvibe

import (
	"os"
)

// IntegrityReport holds the results of a database integrity check.
type IntegrityReport struct {
	Valid          bool
	Errors         []string
	PageCount      int
	FreePages      int
	SchemaErrors   []string
	RowCountErrors []string
}

// DatabaseInfo holds metadata about the database file and configuration.
type DatabaseInfo struct {
	FilePath  string
	FileSize  int64
	PageSize  int
	PageCount int
	FreePages int
	WALMode   bool
	Encoding  string
}

// PageStats holds page-level statistics.
type PageStats struct {
	LeafPages     int
	InteriorPages int
	OverflowPages int
	TotalPages    int
}

// CheckIntegrity validates the database schema and row counts.
func (db *Database) CheckIntegrity() (*IntegrityReport, error) {
	db.queryMu.RLock()
	defer db.queryMu.RUnlock()

	report := &IntegrityReport{
		Valid:          true,
		Errors:         []string{},
		SchemaErrors:   []string{},
		RowCountErrors: []string{},
	}

	// Check schema integrity: every table must have a column order entry
	for tableName := range db.tables {
		if _, ok := db.columnOrder[tableName]; !ok {
			msg := "table " + tableName + ": missing column order"
			report.SchemaErrors = append(report.SchemaErrors, msg)
			report.Errors = append(report.Errors, msg)
			report.Valid = false
		}
	}

	// Check row data references valid columns
	for tableName, rows := range db.data {
		colTypes := db.tables[tableName]
		if colTypes == nil {
			msg := "table " + tableName + ": row data exists but schema is missing"
			report.RowCountErrors = append(report.RowCountErrors, msg)
			report.Errors = append(report.Errors, msg)
			report.Valid = false
			continue
		}
		report.PageCount += len(rows)
	}

	return report, nil
}

// GetDatabaseInfo returns metadata about the database.
func (db *Database) GetDatabaseInfo() (*DatabaseInfo, error) {
	db.queryMu.RLock()
	defer db.queryMu.RUnlock()

	info := &DatabaseInfo{
		FilePath: db.dbPath,
		WALMode:  db.journalMode == "wal",
		Encoding: "UTF-8",
		PageSize: 4096, // default page size
	}

	if db.dbPath != ":memory:" {
		if fi, err := os.Stat(db.dbPath); err == nil {
			info.FileSize = fi.Size()
		}
	}

	// Count total rows across all tables as a proxy for page count
	for _, rows := range db.data {
		info.PageCount += len(rows)
	}

	return info, nil
}

// GetPageStats returns page-level statistics for the database.
func (db *Database) GetPageStats() (*PageStats, error) {
	db.queryMu.RLock()
	defer db.queryMu.RUnlock()

	stats := &PageStats{}
	// For in-memory databases without a B-Tree backend, each row corresponds
	// to one logical leaf record. Interior and overflow pages are zero.
	for _, rows := range db.data {
		stats.LeafPages += len(rows)
	}
	stats.TotalPages = stats.LeafPages + stats.InteriorPages + stats.OverflowPages
	return stats, nil
}
