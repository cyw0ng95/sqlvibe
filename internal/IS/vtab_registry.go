package IS

import (
	"sync"

	DS "github.com/cyw0ng95/sqlvibe/internal/DS"
)

var (
	vtabMu      sync.RWMutex
	vtabModules = map[string]DS.VTabModule{}
)

// RegisterVTabModule registers a virtual table module under the given name.
func RegisterVTabModule(name string, mod DS.VTabModule) {
	vtabMu.Lock()
	vtabModules[name] = mod
	vtabMu.Unlock()
}

// GetVTabModule returns the virtual table module registered under name.
func GetVTabModule(name string) (DS.VTabModule, bool) {
	vtabMu.RLock()
	mod, ok := vtabModules[name]
	vtabMu.RUnlock()
	return mod, ok
}
