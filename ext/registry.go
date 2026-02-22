package ext

import (
	"sort"
	"strings"
	"sync"
)

var (
	mu       sync.RWMutex
	registry = make(map[string]Extension)
	funcMap  = make(map[string]Extension)
)

// Register adds an extension to the global registry and maps its function names.
func Register(name string, e Extension) {
	mu.Lock()
	defer mu.Unlock()
	registry[name] = e
	for _, fn := range e.Functions() {
		funcMap[strings.ToUpper(fn)] = e
	}
}

// Get returns the named extension, or (nil, false) if not found.
func Get(name string) (Extension, bool) {
	mu.RLock()
	defer mu.RUnlock()
	e, ok := registry[name]
	return e, ok
}

// List returns all registered extensions in deterministic name order.
func List() []Extension {
	mu.RLock()
	defer mu.RUnlock()
	names := make([]string, 0, len(registry))
	for n := range registry {
		names = append(names, n)
	}
	sort.Strings(names)
	list := make([]Extension, 0, len(names))
	for _, n := range names {
		list = append(list, registry[n])
	}
	return list
}

// CallFunc dispatches a SQL function call to the registered extension handler.
// Returns (result, true) if a handler is found, (nil, false) otherwise.
func CallFunc(name string, args []interface{}) (interface{}, bool) {
	mu.RLock()
	e, ok := funcMap[strings.ToUpper(name)]
	mu.RUnlock()
	if !ok {
		return nil, false
	}
	return e.CallFunc(name, args), true
}

// AllOpcodes returns all opcodes from all registered extensions.
func AllOpcodes() []Opcode {
	mu.RLock()
	defer mu.RUnlock()
	var ops []Opcode
	for _, e := range registry {
		ops = append(ops, e.Opcodes()...)
	}
	return ops
}

// AllFunctions returns all function names from all registered extensions.
func AllFunctions() []string {
	mu.RLock()
	defer mu.RUnlock()
	var funcs []string
	for _, e := range registry {
		funcs = append(funcs, e.Functions()...)
	}
	return funcs
}
