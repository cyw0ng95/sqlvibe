package fts5

import (
	is "github.com/cyw0ng95/sqlvibe/pkg/sqlvibe/is"
)

func init() {
	is.RegisterVTabModule("fts5", &FTS5Module{})
}
