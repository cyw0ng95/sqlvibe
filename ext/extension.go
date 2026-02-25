package ext

// Opcode represents a VM opcode provided by an extension.
type Opcode struct {
	Name string
	Code int
}

// Extension is the interface all sqlvibe extensions must implement.
// Extensions register SQL functions and optional VM opcodes.
type Extension interface {
	// Name returns the unique extension identifier (e.g., "json", "math").
	Name() string
	// Description returns a human-readable description of the extension.
	Description() string
	// Functions returns the list of SQL function names this extension handles.
	Functions() []string
	// Opcodes returns any custom VM opcodes this extension adds (may be empty).
	Opcodes() []Opcode
	// CallFunc evaluates a SQL function by name with the given argument values.
	// args contains evaluated Go values (int64, float64, string, []byte, nil).
	CallFunc(name string, args []interface{}) interface{}
	// Register is called when a database is opened, allowing the extension
	// to perform additional setup. db is *sqlvibe.Database (passed as interface{}).
	Register(db interface{}) error
	// Close releases any resources held by the extension.
	Close() error
}

// TableFunction represents a SQL table-valued function provided by an extension.
type TableFunction struct {
	Name    string
	MinArgs int
	MaxArgs int // -1 for unlimited
	Rows    func(args []interface{}) ([]map[string]interface{}, error)
}

// AggregateFunction represents a SQL aggregate function provided by an extension.
type AggregateFunction struct {
	Name string
}

// TableFunctionProvider is an optional interface that extensions implement
// to provide table-valued functions.
type TableFunctionProvider interface {
	TableFunctions() []TableFunction
}

// AggregateProvider is an optional interface that extensions implement
// to provide aggregate functions.
type AggregateProvider interface {
	Aggregates() []AggregateFunction
}
