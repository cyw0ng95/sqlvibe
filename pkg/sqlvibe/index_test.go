package sqlvibe

import (
	"testing"
)

func TestCreateIndex(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")

	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"SimpleIndex", "CREATE INDEX idx_name ON users(name)", false},
		{"UniqueIndex", "CREATE UNIQUE INDEX idx_email ON users(email)", false},
		{"MultiColumnIndex", "CREATE INDEX idx_name_email ON users(name, email)", false},
		{"DuplicateIndex", "CREATE INDEX idx_name ON users(name)", true},
		{"IfNotExists", "CREATE INDEX IF NOT EXISTS idx_name ON users(name)", false},
		{"NonExistentTable", "CREATE INDEX idx_x ON nonexistent(col)", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.Exec(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Exec() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	if len(db.indexes) != 3 {
		t.Errorf("expected 3 indexes, got %d", len(db.indexes))
	}
}

func TestDropIndex(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	db.Exec("CREATE INDEX idx_name ON users(name)")

	if len(db.indexes) != 1 {
		t.Fatalf("expected 1 index, got %d", len(db.indexes))
	}

	_, err := db.Exec("DROP INDEX idx_name")
	if err != nil {
		t.Errorf("DROP INDEX failed: %v", err)
	}

	if len(db.indexes) != 0 {
		t.Errorf("expected 0 indexes after DROP, got %d", len(db.indexes))
	}
}
