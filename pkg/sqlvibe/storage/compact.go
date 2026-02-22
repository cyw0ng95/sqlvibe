package storage

import "fmt"

// Compact rebuilds a HybridStore by eliminating tombstone rows and
// returning a fresh, densely-packed store with the same column definitions.
//
// The returned store has:
//   - No deleted rows (LiveCount == RowCount)
//   - A rebuilt ColumnStore (no gaps)
//   - All existing bitmap and skip-list indexes rebuilt for the new row positions
//
// The original store hs is not modified.
func Compact(hs *HybridStore) *HybridStore {
	cols := hs.Columns()

	// Collect the declared value types from each column vector.
	types := make([]ValueType, len(cols))
	for i, col := range cols {
		vec := hs.colStore.GetColumn(col)
		if vec != nil {
			types[i] = vec.Type
		}
	}

	fresh := NewHybridStore(cols, types)

	// Re-insert all live rows.  The new store's row indices start at 0.
	for _, row := range hs.Scan() {
		fresh.Insert(row)
	}

	// Rebuild indexes: copy the index structure definitions, then back-fill.
	for _, col := range hs.indexEngine.BitmapColumns() {
		fresh.CreateIndex(col, false)
	}
	for _, col := range hs.indexEngine.SkipListColumns() {
		fresh.CreateIndex(col, true)
	}

	return fresh
}

// CompactFile reads a SQLVIBE binary database file, compacts it (removes
// tombstones), and writes the result back to the same path.
//
// The schema stored in the file is preserved and used for the rewrite.  If the
// file is already fully compact (no deleted rows) the function is a no-op
// (still rewrites the file to consolidate metadata timestamps).
func CompactFile(path string) error {
	hs, schema, err := ReadDatabase(path)
	if err != nil {
		return fmt.Errorf("compact read: %w", err)
	}
	compacted := Compact(hs)
	if err := WriteDatabase(path, compacted, schema); err != nil {
		return fmt.Errorf("compact write: %w", err)
	}
	return nil
}

// CompactFileOpts is like CompactFile but writes the compacted database with
// the specified compression type.
func CompactFileOpts(path string, compressionType uint32) error {
	hs, schema, err := ReadDatabase(path)
	if err != nil {
		return fmt.Errorf("compact read: %w", err)
	}
	compacted := Compact(hs)
	if err := WriteDatabaseOpts(path, compacted, schema, compressionType); err != nil {
		return fmt.Errorf("compact write: %w", err)
	}
	return nil
}
