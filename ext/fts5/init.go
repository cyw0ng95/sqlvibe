package fts5

import (
	IS "github.com/cyw0ng95/sqlvibe/internal/IS"
)

func init() {
	IS.RegisterVTabModule("fts5", &FTS5Module{})
}
