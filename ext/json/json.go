// Package json implements the sqlvibe JSON extension, providing JSON1-compatible
// SQL functions aligned with https://sqlite.org/json1.html.
//
// Register this extension by building with the SVDB_EXT_JSON build tag:
//
//	go build -tags SVDB_EXT_JSON ./...
package json

import "github.com/cyw0ng95/sqlvibe/ext"

// JSONExtension implements the JSON1-compatible extension.
// The actual implementation is in json_impl.go (CGO).
type JSONExtension struct{}

func (e *JSONExtension) Name() string        { return "json" }
func (e *JSONExtension) Description() string { return "JSON extension" }

func (e *JSONExtension) Functions() []string {
	return []string{
		"json", "json_array", "json_extract", "json_invalid",
		"json_isvalid", "json_valid", "json_length", "json_array_length",
		"json_object", "json_quote", "json_keys",
		"json_remove", "json_replace", "json_set", "json_type", "json_update",
		// v0.9.17 additions
		"jsonb", "jsonb_array", "jsonb_object",
		"json_pretty", "json_patch", "json_array_insert",
		"json_group_array", "json_group_object",
		"jsonb_group_array", "jsonb_group_object",
	}
}

func (e *JSONExtension) Opcodes() []ext.Opcode { return nil }

func (e *JSONExtension) Register(db interface{}) error { return nil }

func (e *JSONExtension) Close() error { return nil }

// CallFunc dispatches to the appropriate implementation based on build tag.
// See json_pure.go or json_cgo.go for the actual implementations.

// Aggregates returns the aggregate functions provided by the JSON extension.
func (e *JSONExtension) Aggregates() []ext.AggregateFunction {
	return []ext.AggregateFunction{
		{Name: "json_group_array"},
		{Name: "jsonb_group_array"},
		{Name: "json_group_object"},
		{Name: "jsonb_group_object"},
	}
}

func init() {
	ext.Register("json", &JSONExtension{})
}
