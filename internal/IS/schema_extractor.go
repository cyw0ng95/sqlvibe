package IS

import (
	"fmt"
	
	"github.com/sqlvibe/sqlvibe/internal/DS"
)

// SchemaExtractor extracts table and column metadata from BTree schema
type SchemaExtractor struct {
	btree *DS.BTree
}

// NewSchemaExtractor creates a new schema extractor
func NewSchemaExtractor(btree *DS.BTree) *SchemaExtractor {
	return &SchemaExtractor{
		btree: btree,
	}
}

// ExtractTables extracts all table information from the database
func (se *SchemaExtractor) ExtractTables() ([]TableInfo, error) {
	tables := make([]TableInfo, 0)
	
	// TODO: Extract table definitions from BTree root page
	// This will:
	// 1. Find the sqlite_master table
	// 2. Query it for all table entries
	// 3. Build TableInfo structs
	
	// For now, return empty list
	return tables, nil
}

// ExtractColumns extracts column information for a specific table
func (se *SchemaExtractor) ExtractColumns(tableName string) ([]ColumnInfo, error) {
	columns := make([]ColumnInfo, 0)
	
	// TODO: Extract column definitions from table page
	// This will:
	// 1. Find the table's root page
	// 2. Parse the table CREATE statement
	// 3. Build ColumnInfo structs with types
	
	// For now, return empty list
	return columns, nil
}

// ExtractViews extracts all view information from the database
func (se *SchemaExtractor) ExtractViews() ([]ViewInfo, error) {
	views := make([]ViewInfo, 0)
	
	// TODO: Extract view definitions from sqlite_master
	// This will:
	// 1. Query sqlite_master for view entries
	// 2. Build ViewInfo structs
	
	// For now, return empty list
	return views, nil
}

// ExtractConstraints extracts constraint information for a table
func (se *SchemaExtractor) ExtractConstraints(tableName string) ([]ConstraintInfo, error) {
	constraints := make([]ConstraintInfo, 0)
	
	// TODO: Extract constraint definitions from BTree
	// This will:
	// 1. Parse CREATE TABLE statement
	// 2. Extract PRIMARY KEY constraints
	// 3. Extract UNIQUE constraints
	// 4. Extract CHECK constraints
	// 5. Extract FOREIGN KEY constraints
	
	// For now, return empty list
	return constraints, nil
}

// IsNullable determines if a column allows NULL values
func IsNullable(sqliteType string) string string {
	// Check if type includes NOT NULL constraint
	if containsWord(sqliteType, "NOT NULL") {
		return "NO"
	}
	return "YES"
}

// containsWord checks if a string contains a word as a whole word
func containsWord(s, word string) bool {
	return containsWordHelper(s, word, 0)
}

// containsWordHelper checks if a string contains a word at a position
func containsWordHelper(s, word string, pos int) bool {
	if pos+len(word) > len(s) {
		return false
	}
	
	// Check word boundary before match
	if pos > 0 && s[pos-1:pos-1] != ' ' && !isLetter(rune(s[pos-1])) {
		return false
	}
	
	// Check word boundary after match
	if pos+len(word) < len(s) && s[pos+len(word):pos+len(word)] != ' ' && !isLetter(rune(s[pos+len(word)])) {
		return false
	}
	
	return s[pos:pos+len(word)] == word
}

// isLetter checks if a rune is a letter
func isLetter(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}
	
	sqliteTypeUpper := ""
	for _, char := range sqliteType {
		sqliteTypeUpper += string(char)
	}
	
	if mapped, ok := typeMap[sqliteTypeUpper]; ok {
		return mapped
	}
	
	// Default fallback
	return sqliteTypeUpper
}

// IsNullable determines if a column allows NULL values
func IsNullable(sqliteType string) string {
	// Check if type includes NOT NULL constraint
	if containsWord(sqliteType, "NOT NULL") {
		return "NO"
	}
	return "YES"
}

// containsWord checks if a string contains a word as a whole word
func containsWord(s, word string) bool {
	return containsWordHelper(s, word, 0)
}

func containsWordHelper(s, word string, pos int) bool {
	if pos+len(word) > len(s) {
		return false
	}
	
	// Check if word matches at this position
	for i := 0; i < len(word); i++ {
		if pos+i >= len(s) || s[pos+i] != word[i] {
			return false
		}
	}
	
	// Check boundaries
	wordBefore := pos == 0 || s[pos-1] == ' ' || s[pos-1] == '\t'
	wordAfter := pos+len(word) == len(s) || s[pos+len(word)] == ' ' || s[pos+len(word)] == '\t'
	
	if !wordBefore && !wordAfter {
		return true
	}
	
	return false
}
