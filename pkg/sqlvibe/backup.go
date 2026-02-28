package sqlvibe

import (
	"fmt"
	"io"
	"os"
	"time"

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

// BackupConfig holds options for BackupToWithConfig.
type BackupConfig struct {
	Progress     bool
	PagesPerStep int
	Callback     func(rowsCopied int) error // Progress callback
}

// BackupTo creates a full backup of the database at the given path.
func (db *Database) BackupTo(path string) error {
	_, err := db.execFullBackup(path)
	return err
}

// BackupToWithConfig creates a full backup with the given config options.
// config is reserved for future use (e.g. progress callbacks, pages-per-step).
func (db *Database) BackupToWithConfig(path string, config BackupConfig) error {
	if config.Callback != nil {
		return db.BackupToWithCallback(path, config.Callback)
	}
	return db.BackupTo(path)
}

// BackupToWithCallback creates a full backup with a progress callback.
// The callback is called after each table is backed up with the number of rows copied so far.
func (db *Database) BackupToWithCallback(path string, callback func(rowsCopied int) error) error {
	if db.dbPath == ":memory:" {
		// Serialize every hybrid store table
		f, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("BACKUP: create dest: %w", err)
		}
		defer f.Close()

		totalRows := 0
		for tableName := range db.tables {
			if err := DS.FullBackup(db.hybridStores[tableName], path+"."+tableName); err != nil {
				return fmt.Errorf("BACKUP: table %q: %w", tableName, err)
			}
			totalRows += len(db.data[tableName])
			if callback != nil {
				if err := callback(totalRows); err != nil {
					return fmt.Errorf("BACKUP: callback error: %w", err)
				}
			}
		}
		// Write manifest
		fmt.Fprintf(f, "sqlvibe-backup tables=%d\n", len(db.tables))
		for tableName := range db.tables {
			fmt.Fprintf(f, "table %s rows=%d\n", tableName, len(db.data[tableName]))
		}
	} else {
		// File-backed: copy the main DB file with progress
		src, err := os.Open(db.dbPath)
		if err != nil {
			return fmt.Errorf("BACKUP: open source: %w", err)
		}
		defer src.Close()

		dst, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("BACKUP: create dest: %w", err)
		}
		defer dst.Close()

		buf := make([]byte, 32*1024) // 32KB chunks
		var totalCopied int64
		for {
			n, err := src.Read(buf)
			if n > 0 {
				if _, werr := dst.Write(buf[:n]); werr != nil {
					return fmt.Errorf("BACKUP: write: %w", werr)
				}
				totalCopied += int64(n)
				if callback != nil {
					if cerr := callback(int(totalCopied)); cerr != nil {
						return fmt.Errorf("BACKUP: callback error: %w", cerr)
					}
				}
			}
			if err != nil {
				if err == io.EOF {
					break
				}
				return fmt.Errorf("BACKUP: read: %w", err)
			}
		}
	}
	return nil
}

// BackupToWriter creates a full backup and writes it to an io.Writer.
// This is useful for streaming backups to network destinations or compression.
func (db *Database) BackupToWriter(w io.Writer) error {
	if db.dbPath == ":memory:" {
		// Write manifest header
		fmt.Fprintf(w, "sqlvibe-backup tables=%d\n", len(db.tables))
		for tableName := range db.tables {
			rows := db.data[tableName]
			fmt.Fprintf(w, "table %s rows=%d\n", tableName, len(rows))
			// Write row data in JSON Lines format
			for _, row := range rows {
				fmt.Fprintf(w, "row ")
				for col, val := range row {
					fmt.Fprintf(w, "%s=%v ", col, val)
				}
				fmt.Fprintf(w, "\n")
			}
		}
	} else {
		// File-backed: stream the file
		f, err := os.Open(db.dbPath)
		if err != nil {
			return fmt.Errorf("BACKUP: open source: %w", err)
		}
		defer f.Close()

		buf := make([]byte, 32*1024)
		for {
			n, err := f.Read(buf)
			if n > 0 {
				if _, werr := w.Write(buf[:n]); werr != nil {
					return fmt.Errorf("BACKUP: write: %w", werr)
				}
			}
			if err != nil {
				if err == io.EOF {
					break
				}
				return fmt.Errorf("BACKUP: read: %w", err)
			}
		}
	}
	return nil
}

// BackupManifest holds metadata about a backup.
type BackupManifest struct {
	Version     string `json:"version"`
	PageSize    int    `json:"page_size"`
	Compression string `json:"compression"`
	TableCount  int    `json:"table_count"`
	RowCount    int64  `json:"row_count"`
	BackupTime  string `json:"backup_time"`
}

// GetBackupManifest returns metadata about the database suitable for backup.
func (db *Database) GetBackupManifest() *BackupManifest {
	totalRows := int64(0)
	for _, rows := range db.data {
		totalRows += int64(len(rows))
	}

	pageSize := db.getPragmaInt("page_size", 4096)
	compression := db.getPragmaStr("compression", "none")

	return &BackupManifest{
		Version:     "v0.10.4",
		PageSize:    int(pageSize),
		Compression: compression,
		TableCount:  len(db.tables),
		RowCount:    totalRows,
		BackupTime:  time.Now().UTC().Format(time.RFC3339),
	}
}
