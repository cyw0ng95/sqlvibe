package math

import (
	"testing"
)

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
			result := evalAbs([]interface{}{tt.input})
			if result != tt.expected {
				t.Errorf("evalAbs(%v) = %v, want %v", tt.input, result, tt.expected)
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
			result := evalCeil([]interface{}{tt.input})
			if result != tt.expected {
				t.Errorf("evalCeil(%v) = %v, want %v", tt.input, result, tt.expected)
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
			result := evalFloor([]interface{}{tt.input})
			if result != tt.expected {
				t.Errorf("evalFloor(%v) = %v, want %v", tt.input, result, tt.expected)
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
			result := evalRound(tt.args)
			if result != tt.expected {
				t.Errorf("evalRound(%v) = %v, want %v", tt.args, result, tt.expected)
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
			result := evalPower(tt.args)
			if result != tt.expected {
				t.Errorf("evalPower(%v) = %v, want %v", tt.args, result, tt.expected)
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
			result := evalSqrt([]interface{}{tt.input})
			if result != tt.expected {
				t.Errorf("evalSqrt(%v) = %v, want %v", tt.input, result, tt.expected)
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
			result := evalMod(tt.args)
			if result != tt.expected {
				t.Errorf("evalMod(%v) = %v, want %v", tt.args, result, tt.expected)
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
			result := evalExp([]interface{}{tt.input})
			if result != tt.expected {
				t.Errorf("evalExp(%v) = %v, want %v", tt.input, result, tt.expected)
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
			result := evalLn([]interface{}{tt.input})
			if result != tt.expected {
				t.Errorf("evalLn(%v) = %v, want %v", tt.input, result, tt.expected)
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
			result := evalLog(tt.args)
			if result != tt.expected {
				t.Errorf("evalLog(%v) = %v, want %v", tt.args, result, tt.expected)
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
			result := evalLog2([]interface{}{tt.input})
			if result != tt.expected {
				t.Errorf("evalLog2(%v) = %v, want %v", tt.input, result, tt.expected)
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
			result := evalLog10([]interface{}{tt.input})
			if result != tt.expected {
				t.Errorf("evalLog10(%v) = %v, want %v", tt.input, result, tt.expected)
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
			result := evalSign([]interface{}{tt.input})
			if result != tt.expected {
				t.Errorf("evalSign(%v) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestEvalRandom(t *testing.T) {
	result := evalRandom([]interface{}{})
	if result == nil {
		t.Error("evalRandom should return a value")
	}
	if _, ok := result.(int64); !ok {
		t.Errorf("evalRandom should return int64, got %T", result)
	}
}

func TestEvalRandombLOB(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected int
	}{
		{"valid size", int64(10), 10},
		{"zero size", int64(0), 0},
		{"negative size", int64(-5), 0},
		{"no args", nil, 0},
		{"invalid", "test", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := evalRandombLOB([]interface{}{tt.input})
			if result == nil {
				t.Fatalf("evalRandombLOB(%v) returned nil", tt.input)
			}
			if b, ok := result.([]byte); !ok {
				t.Errorf("evalRandombLOB should return []byte, got %T", result)
			} else if len(b) != tt.expected {
				t.Errorf("evalRandombLOB(%v) = %d bytes, want %d", tt.input, len(b), tt.expected)
			}
		})
	}
}

func TestEvalZerobLOB(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected int
	}{
		{"valid size", int64(10), 10},
		{"zero size", int64(0), 0},
		{"negative size", int64(-5), 0},
		{"no args", nil, 0},
		{"invalid", "test", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := evalZerobLOB([]interface{}{tt.input})
			if result == nil {
				t.Fatalf("evalZerobLOB(%v) returned nil", tt.input)
			}
			if b, ok := result.([]byte); !ok {
				t.Errorf("evalZerobLOB should return []byte, got %T", result)
			} else if len(b) != tt.expected {
				t.Errorf("evalZerobLOB(%v) = %d bytes, want %d", tt.input, len(b), tt.expected)
			}
		})
	}
}

func TestToFloat64Math(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected float64
		ok       bool
	}{
		{int64(42), 42.0, true},
		{int(42), 42.0, true},
		{float64(3.14), 3.14, true},
		{nil, 0, false},
		{"test", 0, false},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result, ok := toFloat64Math(tt.input)
			if ok != tt.ok {
				t.Errorf("toFloat64Math(%v) ok = %v, want %v", tt.input, ok, tt.ok)
			}
			if ok && result != tt.expected {
				t.Errorf("toFloat64Math(%v) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestToInt64Math(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected int64
		ok       bool
	}{
		{int64(42), 42, true},
		{int(42), 42, true},
		{float64(3.14), 3, true},
		{nil, 0, false},
		{"test", 0, false},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result, ok := toInt64Math(tt.input)
			if ok != tt.ok {
				t.Errorf("toInt64Math(%v) ok = %v, want %v", tt.input, ok, tt.ok)
			}
			if ok && result != tt.expected {
				t.Errorf("toInt64Math(%v) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}
