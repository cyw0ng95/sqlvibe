package sqlvibe

import (
	"fmt"
	"io"
	"os"

	"github.com/cyw0ng95/sqlvibe/internal/DS"
	"github.com/cyw0ng95/sqlvibe/internal/QP"
)

// handleBackup executes a BACKUP DATABASE TO 'path' or BACKUP INCREMENTAL TO 'path' statement.
func (db *Database) handleBackup(stmt *QP.BackupStmt) (*Rows, error) {
	if stmt.DestPath == "" {
		return nil, fmt.Errorf("BACKUP: destination path is required")
	}

	if stmt.Incremental {
		return db.execIncrementalBackup(stmt.DestPath)
	}
	return db.execFullBackup(stmt.DestPath)
}

// execFullBackup writes the entire database to the destination path.
// For in-memory databases the serialised row data is written for each table.
// For file-backed databases the data file is copied.
func (db *Database) execFullBackup(destPath string) (*Rows, error) {
	rowsCopied := 0

	if db.dbPath == ":memory:" {
		// Serialise every hybrid store table that exists.
		f, err := os.Create(destPath)
		if err != nil {
			return nil, fmt.Errorf("BACKUP: create dest: %w", err)
		}
		defer f.Close()

		for tableName, hs := range db.hybridStores {
			if err := DS.FullBackup(hs, destPath+"."+tableName); err != nil {
				return nil, fmt.Errorf("BACKUP: table %q: %w", tableName, err)
			}
		}
		// Write a simple manifest line to the main dest file.
		fmt.Fprintf(f, "sqlvibe-backup tables=%d\n", len(db.tables))
		for tableName := range db.tables {
			rowsCopied += len(db.data[tableName])
			fmt.Fprintf(f, "table %s rows=%d\n", tableName, len(db.data[tableName]))
		}
	} else {
		// File-backed: copy the main DB file.
		src, err := os.Open(db.dbPath)
		if err != nil {
			return nil, fmt.Errorf("BACKUP: open source: %w", err)
		}
		defer src.Close()

		dst, err := os.Create(destPath)
		if err != nil {
			return nil, fmt.Errorf("BACKUP: create dest: %w", err)
		}
		defer dst.Close()

		n, err := io.Copy(dst, src)
		if err != nil {
			return nil, fmt.Errorf("BACKUP: copy: %w", err)
		}
		rowsCopied = int(n) // bytes copied
	}

	return &Rows{
		Columns: []string{"pages_copied"},
		Data:    [][]interface{}{{int64(rowsCopied)}},
	}, nil
}

// execIncrementalBackup copies only changed rows since the last backup.
func (db *Database) execIncrementalBackup(destPath string) (*Rows, error) {
	totalCopied := 0
	for tableName, hs := range db.hybridStores {
		ib := DS.NewIncrementalBackup(hs, destPath+"."+tableName)
		if err := ib.Start(); err != nil {
			return nil, fmt.Errorf("BACKUP INCREMENTAL: start table %q: %w", tableName, err)
		}
		n, err := ib.Next()
		if err != nil {
			return nil, fmt.Errorf("BACKUP INCREMENTAL: next table %q: %w", tableName, err)
		}
		if err := ib.Close(); err != nil {
			return nil, fmt.Errorf("BACKUP INCREMENTAL: close table %q: %w", tableName, err)
		}
		totalCopied += n
	}

	return &Rows{
		Columns: []string{"rows_copied"},
		Data:    [][]interface{}{{int64(totalCopied)}},
	}, nil
}
