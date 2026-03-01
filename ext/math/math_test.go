package math

import (
	"math"
	"reflect"
	"testing"
)

// compareValues compares two interface{} values, handling numeric type conversions
func compareValues(got, want interface{}) bool {
	if got == nil && want == nil {
		return true
	}
	if got == nil || want == nil {
		return false
	}
	
	// Use reflection to compare values
	gv := reflect.ValueOf(got)
	wv := reflect.ValueOf(want)
	
	// Convert numeric types to float64 for comparison
	if gv.Kind() >= reflect.Int && gv.Kind() <= reflect.Uint64 {
		got = float64(gv.Int())
		gv = reflect.ValueOf(got)
	}
	if wv.Kind() >= reflect.Int && wv.Kind() <= reflect.Uint64 {
		want = float64(wv.Int())
		wv = reflect.ValueOf(want)
	}
	
	// Compare floats with tolerance
	if gv.Kind() == reflect.Float64 && wv.Kind() == reflect.Float64 {
		gf := gv.Float()
		wf := wv.Float()
		return math.Abs(gf-wf) < 0.0001
	}
	
	return reflect.DeepEqual(got, want)
}

func TestEvalAbs(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected interface{}
	}{
		{"positive int", int64(5), int64(5)},
		{"negative int", int64(-5), int64(5)},
		{"zero int", int64(0), int64(0)},
		{"positive float", float64(3.14), float64(3.14)},
		{"negative float", float64(-3.14), float64(3.14)},
		{"zero float", float64(0.0), float64(0.0)},
		{"nil", nil, nil},
		{"string returns original", "test", "test"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := callAbs([]interface{}{tt.input})
			if !compareValues(result, tt.expected) {
				t.Errorf("callAbs(%v) = %v (%T), want %v (%T)", tt.input, result, result, tt.expected, tt.expected)
			}
		})
	}
}

func TestEvalCeil(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected interface{}
	}{
		{"positive int", int64(5), int64(5)},
		{"negative int", int64(-5), int64(-5)},
		{"positive float", float64(3.14), float64(4.0)},
		{"negative float", float64(-3.14), float64(-3.0)},
		{"nil", nil, nil},
		{"string returns original", "test", "test"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := callCeil([]interface{}{tt.input})
			if !compareValues(result, tt.expected) {
				t.Errorf("callCeil(%v) = %v (%T), want %v (%T)", tt.input, result, result, tt.expected, tt.expected)
			}
		})
	}
}

func TestEvalFloor(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected interface{}
	}{
		{"positive int", int64(5), int64(5)},
		{"negative int", int64(-5), int64(-5)},
		{"positive float", float64(3.14), float64(3.0)},
		{"negative float", float64(-3.14), float64(-4.0)},
		{"nil", nil, nil},
		{"string returns original", "test", "test"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := callFloor([]interface{}{tt.input})
			if !compareValues(result, tt.expected) {
				t.Errorf("callFloor(%v) = %v (%T), want %v (%T)", tt.input, result, result, tt.expected, tt.expected)
			}
		})
	}
}

func TestEvalRound(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected interface{}
	}{
		{"int zero decimals", []interface{}{int64(42)}, int64(42)},
		{"float zero decimals", []interface{}{float64(3.6)}, float64(4.0)},
		{"float negative decimals", []interface{}{float64(123.45), int64(-1)}, float64(120.0)},
		{"nil", []interface{}{nil}, nil},
		{"no args", []interface{}{}, nil},
		{"string returns original", []interface{}{"test"}, "test"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := callRound(tt.args)
			if !compareValues(result, tt.expected) {
				t.Errorf("callRound(%v) = %v, want %v", tt.args, result, tt.expected)
			}
		})
	}
}

func TestEvalPower(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected interface{}
	}{
		{"2^3", []interface{}{float64(2), float64(3)}, float64(8.0)},
		{"10^0", []interface{}{float64(10), float64(0)}, float64(1.0)},
		{"negative base", []interface{}{float64(-2), float64(3)}, float64(-8.0)},
		{"zero base", []interface{}{float64(0), float64(5)}, float64(0.0)},
		{"no args", []interface{}{}, nil},
		{"one arg", []interface{}{float64(2)}, nil},
		{"invalid base", []interface{}{"a", float64(2)}, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := callPower(tt.args)
			if !compareValues(result, tt.expected) {
				t.Errorf("callPower(%v) = %v, want %v", tt.args, result, tt.expected)
			}
		})
	}
}

func TestEvalSqrt(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected interface{}
	}{
		{"positive", float64(4), float64(2.0)},
		{"zero", float64(0), float64(0.0)},
		{"no args", nil, nil},
		{"negative", float64(-4), nil},
		{"invalid", "test", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := callSqrt([]interface{}{tt.input})
			if !compareValues(result, tt.expected) {
				t.Errorf("callSqrt(%v) = %v (%T), want %v (%T)", tt.input, result, result, tt.expected, tt.expected)
			}
		})
	}
}

func TestEvalMod(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected interface{}
	}{
		{"10 mod 3", []interface{}{int64(10), int64(3)}, int64(1)},
		{"zero divisor", []interface{}{int64(10), int64(0)}, nil},
		{"float mod", []interface{}{float64(10.5), float64(3.0)}, float64(1.5)},
		{"no args", []interface{}{}, nil},
		{"one arg", []interface{}{int64(10)}, nil},
		{"invalid args", []interface{}{"a", int64(3)}, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := callMod(tt.args)
			if !compareValues(result, tt.expected) {
				t.Errorf("callMod(%v) = %v, want %v", tt.args, result, tt.expected)
			}
		})
	}
}

func TestEvalExp(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected interface{}
	}{
		{"zero", float64(0), float64(1.0)},
		{"one", float64(1), float64(2.718281828459045)},
		{"no args", nil, nil},
		{"invalid", "test", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := callExp([]interface{}{tt.input})
			if !compareValues(result, tt.expected) {
				t.Errorf("callExp(%v) = %v (%T), want %v (%T)", tt.input, result, result, tt.expected, tt.expected)
			}
		})
	}
}

func TestEvalLn(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected interface{}
	}{
		{"e", float64(2.718281828459045), float64(1.0)},
		{"one", float64(1), float64(0.0)},
		{"zero", float64(0), nil},
		{"negative", float64(-1), nil},
		{"no args", nil, nil},
		{"invalid", "test", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := callLn([]interface{}{tt.input})
			if !compareValues(result, tt.expected) {
				t.Errorf("callLn(%v) = %v (%T), want %v (%T)", tt.input, result, result, tt.expected, tt.expected)
			}
		})
	}
}

func TestEvalLog(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected interface{}
	}{
		{"log(e)", []interface{}{float64(2.718281828459045)}, float64(1.0)},
		{"log2(4)", []interface{}{float64(2), float64(4)}, float64(2.0)},
		{"invalid base", []interface{}{float64(1), float64(4)}, nil},
		{"zero base", []interface{}{float64(0), float64(4)}, nil},
		{"negative base", []interface{}{float64(-2), float64(4)}, nil},
		{"no args", []interface{}{}, nil},
		{"one arg", []interface{}{float64(4)}, float64(1.3862943611198906)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := callLog(tt.args)
			if !compareValues(result, tt.expected) {
				t.Errorf("callLog(%v) = %v, want %v", tt.args, result, tt.expected)
			}
		})
	}
}

func TestEvalLog2(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected interface{}
	}{
		{"four", float64(4), float64(2.0)},
		{"one", float64(1), float64(0.0)},
		{"zero", float64(0), nil},
		{"negative", float64(-1), nil},
		{"no args", nil, nil},
		{"invalid", "test", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := callLog2([]interface{}{tt.input})
			if !compareValues(result, tt.expected) {
				t.Errorf("callLog2(%v) = %v (%T), want %v (%T)", tt.input, result, result, tt.expected, tt.expected)
			}
		})
	}
}

func TestEvalLog10(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected interface{}
	}{
		{"100", float64(100), float64(2.0)},
		{"one", float64(1), float64(0.0)},
		{"ten", float64(10), float64(1.0)},
		{"zero", float64(0), nil},
		{"negative", float64(-1), nil},
		{"no args", nil, nil},
		{"invalid", "test", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := callLog10([]interface{}{tt.input})
			if !compareValues(result, tt.expected) {
				t.Errorf("callLog10(%v) = %v (%T), want %v (%T)", tt.input, result, result, tt.expected, tt.expected)
			}
		})
	}
}

func TestEvalSign(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected interface{}
	}{
		{"positive int", int64(5), int64(1)},
		{"negative int", int64(-5), int64(-1)},
		{"zero int", int64(0), int64(0)},
		{"positive float", float64(5.0), float64(1.0)},
		{"negative float", float64(-5.0), float64(-1.0)},
		{"zero float", float64(0.0), float64(0.0)},
		{"nil", nil, nil},
		{"string returns nil", "test", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := callSign([]interface{}{tt.input})
			if !compareValues(result, tt.expected) {
				t.Errorf("callSign(%v) = %v (%T), want %v (%T)", tt.input, result, result, tt.expected, tt.expected)
			}
		})
	}
}

func TestEvalRandom(t *testing.T) {
	result := callRandom()
	if result == nil {
		t.Error("callRandom should return a value")
	}
	if _, ok := result.(int64); !ok {
		t.Errorf("callRandom should return int64, got %T", result)
	}
}
