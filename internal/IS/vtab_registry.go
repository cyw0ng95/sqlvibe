package is

import (
	"sync"

	DS "github.com/cyw0ng95/sqlvibe/internal/DS"
)

var (
	vtabModules   = make(map[string]DS.VTabModule)
	vtabModulesMu sync.RWMutex
)

// RegisterVTabModule registers a virtual table module
func RegisterVTabModule(name string, module DS.VTabModule) {
	vtabModulesMu.Lock()
	defer vtabModulesMu.Unlock()
	vtabModules[name] = module
}

// GetVTabModule gets a virtual table module
func GetVTabModule(name string) (DS.VTabModule, bool) {
	vtabModulesMu.RLock()
	defer vtabModulesMu.RUnlock()
	mod, found := vtabModules[name]
	return mod, found
}

// ListVTabModules returns the names of all registered virtual table modules.
func ListVTabModules() []string {
	vtabModulesMu.RLock()
	defer vtabModulesMu.RUnlock()
	names := make([]string, 0, len(vtabModules))
	for name := range vtabModules {
		names = append(names, name)
	}
	return names
}
