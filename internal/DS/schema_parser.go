package DS

import (
	"fmt"
	"strings"
	
	"github.com/sqlvibe/sqlvibe/internal/DS/btree"
)

// SchemaParser parses CREATE TABLE statements
type SchemaParser struct {
	pm  *btree.BTree
}

// NewSchemaParser creates a new schema parser
func NewSchemaParser(btree *btree.BTree) *SchemaParser {
	return &SchemaParser{
		pm: btree,
	}
}

// ParseSchema parses CREATE TABLE statement from SQL string
func (sp *SchemaParser) ParseSchema(sql string) ([]string, error) {
	return []string{sql}
}

// ParseTables parses all CREATE TABLE statements from SQL string
func (sp *SchemaParser) ParseTables(sql string) ([][]string, error) {
	tables := make([][]string, 0)
	
	for _, table := range strings.Split(sql, ";") {
		if strings.TrimSpace(table) == "" {
			continue
		}
		tables = append(tables, table)
	}
	
	return tables, nil
}

// ParseTable parses a single CREATE TABLE statement
func (sp *SchemaParser) ParseTable(sql string) ([]string, error) {
	parts := strings.FieldsFunc(strings.TrimSpace(sql), " ",")
	if len(parts) != 3 {
		return nil, fmt.Errorf("not a CREATE TABLE statement: %s", sql)
	}
	
	// Expected: CREATE TABLE table_name (col1 type1, col2 type2, ...)
	
	// TODO: Implement actual CREATE TABLE parsing
	// This will:
	// 1. Parse table name
	// 2. Parse column definitions
	// 3. Build TableInfo struct
	// 4. Return TableInfo
	
	return nil, nil
}

// ParseColumnDefinition parses "col_name TYPE NOT NULL DEFAULT"
func (sp *SchemaParser) ParseColumnDefinition(def string) ([]string, error) {
	// TODO: Implement column definition parsing
	// This will parse:
	// - Column name
	// - Column type
	// - NOT NULL
	// - DEFAULT value
	// - Any constraints
	
	return []string{def}, nil
}

// NewSchemaParser creates a new schema parser
func NewSchemaParser(pm *BTreeManager) *SchemaParser {
	return &SchemaParser{
		pm:      pm,
		decoder: encoding.NewDecoder(),
	}
}

// ParseSchema extracts all schema information from BTree
func (sp *SchemaParser) ParseSchema() ([]IS.TableInfo, []IS.ColumnInfo, []IS.ConstraintInfo, []IS.ViewInfo, []IS.ReferentialConstraint, error) {
	tables := make([]IS.TableInfo, 0)
	columns := make([]IS.ColumnInfo, 0)
	constraints := make([]IS.ConstraintInfo, 0)
	views := make([]IS.ViewInfo, 0)
	refs := make([]IS.ReferentialConstraint, 0)

	// TODO: Implement schema extraction from BTree
	// This will:
	// 1. Query sqlite_master table (if it exists)
	// 2. Parse all CREATE TABLE statements
	// 3. Build TableInfo, ColumnInfo, ConstraintInfo structs

	// For now, return empty results
	return tables, columns, constraints, views, refs, nil
}

// NewSchemaParser creates a new schema parser
func NewSchemaParser(tree *DS.BTree) *SchemaParser {
	return &SchemaParser{
		tree: tree,
	}
}

// ParseSchema parses and returns schema information from sqlite_master
func (sp *SchemaParser) ParseSchema() ([]IS.TableInfo, []IS.ColumnInfo, []IS.ViewInfo, []IS.ConstraintInfo, []IS.ReferentialConstraint, error) {
	tables := make([]IS.TableInfo, 0)

	// TODO: Implement actual schema parsing
	// This will:
	// 1. Query sqlite_master table
	// 2. Parse CREATE TABLE statements
	// 3. Extract column definitions
	// 4. Build TableInfo structs
	// 5. Extract constraint information

	return tables, nil
}

// NewSchemaParser creates a new schema parser
func NewSchemaParser(pm *PageManager, decoder *encoding.Decoder) *SchemaParser {
	return &SchemaParser{
		pm:      pm,
		decoder: decoder,
	}
}

// ParseSchema extracts and parses all schema from database
func (sp *SchemaParser) ParseSchema() ([]IS.TableInfo, error) {
	tables := make([]IS.TableInfo, 0)

	// TODO: Query sqlite_master table (if it exists)
	// TODO: Parse CREATE TABLE statements
	// TODO: Build TableInfo structs for each table
	// TODO: Parse column definitions
	// TODO: Parse constraint definitions

	return tables, nil
}

// FindMasterPageLocateor finds the page containing sqlite_master table
func (sp *SchemaParser) FindMasterPageLocateor() (uint32, error) {
	// Search for sqlite_master table in BTree
	// TODO: Scan pages and find table type

	// TODO: Implement BTree scanning
	// For now, just return 0 (page not found)

	return 0, nil
}

// parseCreateTable parses a CREATE TABLE statement
func (sp *SchemaParser) parseCreateTable(sql string) (*IS.TableInfo, error) {
	// TODO: Implement CREATE TABLE parsing
	// This will extract table name and column definitions

	return nil, fmt.Errorf("CREATE TABLE parsing not yet implemented")
}

// parseColumnDefinitions parses column definitions from SQL statement
func (sp *SchemaParser) parseColumnDefinitions(sql string) ([]IS.ColumnInfo, error) {
	// TODO: Implement column definition parsing
	// This will extract column names, types, constraints

	return nil, fmt.Errorf("Column definition parsing not yet implemented")
}

// NewSchemaParser creates a new schema parser
func NewSchemaParser(pm *PageManager) *SchemaParser {
	return &SchemaParser{
		pm:      pm,
		decoder: encoding.NewDecoder(),
	}
}

// ParseSchema extracts all schema information from BTree
func (sp *SchemaParser) ParseSchema() ([]IS.TableInfo, []IS.ColumnInfo, error) {
	tables := make([]IS.TableInfo, 0)
	allColumns := make([]IS.ColumnInfo, 0)

	// Query sqlite_master table (if it exists)
	masterPageNum, err := sp.findMasterTablePage()
	if err != nil {
		return nil, err
	}
	if masterPageNum == 0 {
		// No tables in database
		return tables, allColumns, nil
	}

	// Read and parse master page
	page, err := sp.pm.ReadPage(masterPageNum)
	if err != nil {
		return nil, err
	}
	defer sp.pm.ReleasePage(page)

	// Skip first row (sqlite_master header)
	cursor := newBTreeCursor(page, sp.pm)
	cursor.SkipRow()

	// Parse all CREATE TABLE statements
	for cursor.Next() {
		sql, err := cursor.ReadString()
		if err != nil {
			return nil, err
		}

		if strings.HasPrefix(strings.TrimSpace(sql), "CREATE TABLE") {
			// Parse CREATE TABLE statement
			tableInfo, err := sp.parseCreateTable(sql)
			if err != nil {
				return nil, err
			}

			tables = append(tables, tableInfo)

			// Extract columns from CREATE TABLE
			columns, err := sp.parseCreateColumns(tableInfo.SqlStatement)
			if err != nil {
				return tables, allColumns, err
			}

			// Add columns to global list
			for _, col := range columns {
				allColumns = append(allColumns, col)
			}
		}

		// Skip views for now (will be added later)
		if strings.HasPrefix(sql, "CREATE VIEW") {
			cursor.SkipRow() // Skip CREATE VIEW statements
		}

		// Skip indexes, triggers, etc. for now
		if strings.HasPrefix(sql, "CREATE INDEX") ||
			strings.HasPrefix(sql, "CREATE TRIGGER") ||
			strings.HasPrefix(sql, "CREATE UNIQUE") ||
			strings.HasPrefix(sql, "CREATE VIRTUAL TABLE") {
			cursor.SkipRow()
		}
	}

	return tables, allColumns, nil
}

// findMasterTablePage finds the page containing sqlite_master
func (sp *SchemaParser) findMasterTablePage() (uint32, error) {
	// Start from page 1
	for pageNum := uint32(1); pageNum <= sp.pm.NumPages(); pageNum++ {
		page, err := sp.pm.ReadPage(pageNum)
		if err != nil {
			return 0, err
		}

		// Check each cell for "sqlite_master" string
		for cellOff := uint16(0); cellOff < sp.pm.PageSize(); cellOff++ {
			cellData := page.Data[cellOff : cellOff+8]

			// Decode cell
			cellValue := string(cellData)

			// Check if this is the sqlite_master table
			if cellValue == "sqlite_master" {
				return pageNum, nil
			}
		}
	}

	return 0, fmt.Errorf("sqlite_master table not found")
}

// parseCreateTable parses a CREATE TABLE statement
func (sp *SchemaParser) parseCreateTable(sql string) (*IS.TableInfo, error) {
	// Format: CREATE TABLE [IF NOT EXISTS] table_name (column1 type1, column2 type2, ...) [table_constraint])

	// Extract table name
	tableName := sp.extractTableName(sql)
	if tableName == "" {
		return nil, fmt.Errorf("could not parse table name from: %s", sql)
	}

	// Extract column definitions
	sqlWithoutCreate := strings.TrimPrefix(sql, "CREATE TABLE "+len(tableName))

	// Remove table constraints for now (will parse separately)
	parenPos := strings.Index(sqlWithoutCreate, "(")
	if parenPos != -1 {
		sqlWithoutCreate = sqlWithoutCreate[:parenPos]
	} else {
		// No table constraints
		sqlWithoutCreate = sqlWithoutCreate
	}

	// Parse column definitions
	columnDefs := sp.parseColumnDefinitions(sqlWithoutCreate)

	columns := make([]IS.ColumnInfo, 0)
	for _, def := range columnDefs {
		col := IS.ColumnInfo{
			ColumnName:    def.name,
			TableName:     tableName,
			TableSchema:   IS.TableSchemaMain,
			DataType:      IS.GetSQLiteType(def.typeName),
			IsNullable:    IS.IsNullable(def.typeName),
			ColumnDefault: def.defaultValue,
		}
		columns = append(columns, col)
	}

	tableInfo := &IS.TableInfo{
		TableName:    tableName,
		TableSchema:  IS.TableSchemaMain,
		TableType:    IS.ViewTypeBaseTable,
		SqlStatement: sql,
	}

	return tableInfo, nil
}

// extractTableName extracts table name from CREATE TABLE statement
func (sp *SchemaParser) extractTableName(sql string) string {
	// Format: CREATE TABLE [IF NOT EXISTS] table_name
	// Find "CREATE TABLE" keyword
	createIdx := strings.Index(strings.ToUpper(sql), "CREATE TABLE")
	if createIdx == -1 {
		return "", fmt.Errorf("CREATE TABLE not found")
	}

	// Find table name after "CREATE TABLE"
	afterCreate := strings.TrimSpace(sql[createIdx+12:])

	// Extract table name (first word after CREATE TABLE)
	tableName, err := sp.extractFirstWord(afterCreate)
	if err != nil {
		return "", err
	}

	return tableName
}

// parseColumnDefinitions parses column definitions from CREATE TABLE
func (sp *SchemaParser) parseColumnDefinitions(sql string) ([]IS.ColumnDefinition, error) {
	definitions := []IS.ColumnDefinition{}

	// Split by comma to get individual column definitions
	columnParts := strings.Split(sql, ",")
	for _, part := range columnParts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Parse column definition: name [type] [constraints]
		def := IS.ColumnDefinition{}

		// Extract name (first word)
		name, err := sp.extractFirstWord(part)
		if err != nil {
			return nil, err
		}
		def.name = name

		// Parse remaining definition: [type] [constraints]
		remaining := strings.TrimSpace(part[len(name):])
		if remaining == "" {
			// Only name provided
			definitions = append(definitions, def)
			continue
		}

		// Split on whitespace to separate type from constraints
		parts := strings.FieldsFunc(strings.TrimLeft(remaining, " \t"), 2)
		if len(parts) < 2 {
			// Invalid column definition
			continue
		}

		// parts[0]: type
		typeExpr := strings.TrimSpace(parts[0])
		def.typeName = typeExpr

		// parts[1]: constraints (optional)
		if len(parts) > 1 {
			constraints := strings.TrimSpace(parts[1])
			def.constraints = sp.parseConstraints(constraints)
		}

		definitions = append(definitions, def)
	}

	return definitions, nil
}

// extractFirstWord extracts the first word from a string
func (sp *SchemaParser) extractFirstWord(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", nil
	}

	// Find first word (alphanumeric + underscore)
	return extractFirstWordHelper(s, 0)
}

// extractFirstWordHelper extracts first word from string
func extractFirstWordHelper(s string, pos int) (string, error) {
	if pos >= len(s) {
		return "", nil
	}

	// Skip leading non-alphanumeric characters
	for pos < len(s) && (!isAlphaNum(s[pos]) || s[pos] == '_') {
		pos++
	}

	if pos >= len(s) {
		return "", nil
	}

	start := pos
	for pos < len(s) && (isAlphaNum(s[pos]) || s[pos] == '_') {
		start = pos
		pos++
	}

	if start >= len(s) {
		return "", nil
	}

	return s[start:pos], nil
}

// isAlphaNum checks if character is alphanumeric or underscore
func isAlphaNum(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
}

// parseConstraints parses table constraints
func (sp *SchemaParser) parseConstraints(constraintsStr string) []string {
	if constraintsStr == "" {
		return []string{}
	}

	// For now, just return empty slice
	// TODO: Implement PRIMARY KEY, UNIQUE, CHECK, FOREIGN KEY parsing

	return []string{constraintsStr}
}

// NewBTreeCursor creates a new BTree cursor
func newBTreeCursor(page *DS.Page, pm *PageManager) *BTreeCursor {
	cursor := &BTreeCursor{
		page: page,
		pm:   pm,
	}
	return cursor
}

type BTreeCursor struct {
	page *DS.Page
	pm   *PageManager
}

func (c *BTreeCursor) Next() bool {
	return false // TODO: Implement cursor navigation
}

func (c *BTreeCursor) SkipRow() {
	// Skip this row (sqlite_master header)
	// TODO: Implement proper cursor skip
}

func (c *BTreeCursor) ReadString() (string, error) {
	// TODO: Read cell data as string
	// For now, return empty
	return "", nil
}

func (c *BTreeCursor) SkipCell() {
	// TODO: Skip cells
}

func (c *BTreeCursor) SkipColumn() {
	// TODO: Skip columns
}
