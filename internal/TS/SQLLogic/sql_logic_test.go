package SQLLogic

import (
	"path/filepath"
	"runtime"
	"testing"
)

// testdataDir returns the absolute path to the testdata directory next to
// this source file, so tests work regardless of the working directory.
func testdataDir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "testdata")
}

// TestSQLLogic runs all *.test files found in the testdata directory.
func TestSQLLogic(t *testing.T) {
	RunDir(t, testdataDir())
}
