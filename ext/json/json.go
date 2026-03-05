//go:build SVDB_EXT_JSON

// Package json enables the sqlvibe JSON extension.
// Import this package for its side effects to make the JSON SQL functions
// (json_extract, json_set, json_array, json_object, json_each, json_tree,
// json_group_array, json_group_object, etc.) available in sqlvibe databases.
// The extension is implemented in C++ and registered automatically when the
// package is imported.
package json
