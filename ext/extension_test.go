package ext_test

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/ext"
)

// mockExtension is a minimal Extension used for registry tests.
type mockExtension struct {
	name  string
	funcs []string
}

func (m *mockExtension) Name() string          { return m.name }
func (m *mockExtension) Description() string   { return "mock " + m.name }
func (m *mockExtension) Functions() []string   { return m.funcs }
func (m *mockExtension) Opcodes() []ext.Opcode { return nil }
func (m *mockExtension) CallFunc(name string, args []interface{}) interface{} {
	return "mock_" + name
}
func (m *mockExtension) Register(db interface{}) error { return nil }
func (m *mockExtension) Close() error                  { return nil }

func TestRegistry_Register(t *testing.T) {
	e := &mockExtension{name: "test_reg", funcs: []string{"mock_func"}}
	ext.Register("test_reg", e)

	got, ok := ext.Get("test_reg")
	if !ok {
		t.Fatal("expected extension 'test_reg' to be found")
	}
	if got.Name() != "test_reg" {
		t.Errorf("got name %q, want %q", got.Name(), "test_reg")
	}
}

func TestRegistry_Get_Missing(t *testing.T) {
	_, ok := ext.Get("no_such_extension_xyz")
	if ok {
		t.Error("expected false for missing extension")
	}
}

func TestRegistry_List(t *testing.T) {
	ext.Register("list_ext_a", &mockExtension{name: "list_ext_a", funcs: []string{"fa"}})
	ext.Register("list_ext_b", &mockExtension{name: "list_ext_b", funcs: []string{"fb"}})

	list := ext.List()
	found := map[string]bool{}
	for _, e := range list {
		found[e.Name()] = true
	}
	if !found["list_ext_a"] {
		t.Error("list_ext_a not found in List()")
	}
	if !found["list_ext_b"] {
		t.Error("list_ext_b not found in List()")
	}
}

func TestRegistry_CallFunc(t *testing.T) {
	ext.Register("callfunc_ext", &mockExtension{
		name:  "callfunc_ext",
		funcs: []string{"callfunc_fn"},
	})

	result, ok := ext.CallFunc("callfunc_fn", []interface{}{"arg1"})
	if !ok {
		t.Fatal("expected CallFunc to return ok=true")
	}
	if result != "mock_callfunc_fn" {
		t.Errorf("unexpected result: %v", result)
	}
}

func TestRegistry_CallFunc_Missing(t *testing.T) {
	_, ok := ext.CallFunc("no_such_func_xyz", nil)
	if ok {
		t.Error("expected ok=false for unregistered function")
	}
}

func TestRegistry_AllFunctions(t *testing.T) {
	ext.Register("allfuncs_ext", &mockExtension{
		name:  "allfuncs_ext",
		funcs: []string{"af1", "af2"},
	})

	funcs := ext.AllFunctions()
	found := map[string]bool{}
	for _, f := range funcs {
		found[f] = true
	}
	if !found["af1"] {
		t.Error("af1 not in AllFunctions()")
	}
	if !found["af2"] {
		t.Error("af2 not in AllFunctions()")
	}
}
