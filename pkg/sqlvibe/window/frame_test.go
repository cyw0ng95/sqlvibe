package window_test

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe/window"
)

func TestResolveFrameBounds_Default(t *testing.T) {
	// Default (nil frame): from 0 to current pos
	start, end := window.ResolveFrameBounds(nil, 2, 5)
	if start != 0 || end != 2 {
		t.Errorf("ResolveFrameBounds(nil, 2, 5) = (%d, %d), want (0, 2)", start, end)
	}
}

func TestResolveFrameBounds_UnboundedPrecedingToCurrentRow(t *testing.T) {
	frame := &QP.WindowFrame{
		Start: QP.FrameBound{Type: "UNBOUNDED"},
		End:   QP.FrameBound{Type: "CURRENT"},
	}
	start, end := window.ResolveFrameBounds(frame, 3, 5)
	if start != 0 || end != 3 {
		t.Errorf("ResolveFrameBounds(UNBOUNDED PRECEDING TO CURRENT) = (%d, %d), want (0, 3)", start, end)
	}
}

func TestResolveFrameBounds_PrecedingToFollowing(t *testing.T) {
	frame := &QP.WindowFrame{
		Start: QP.FrameBound{Type: "PRECEDING", Value: &QP.Literal{Value: int64(1)}},
		End:   QP.FrameBound{Type: "FOLLOWING", Value: &QP.Literal{Value: int64(1)}},
	}
	start, end := window.ResolveFrameBounds(frame, 2, 5)
	if start != 1 || end != 3 {
		t.Errorf("ResolveFrameBounds(1 PRECEDING TO 1 FOLLOWING, pos=2) = (%d, %d), want (1, 3)", start, end)
	}
}

func TestResolveFrameBounds_EmptyRange(t *testing.T) {
	// Empty total
	start, end := window.ResolveFrameBounds(nil, 0, 0)
	if start != 0 || end != 0 {
		t.Errorf("ResolveFrameBounds empty = (%d, %d), want (0, 0)", start, end)
	}
}

func TestResolveFramePos_Unbounded_Start(t *testing.T) {
	fb := QP.FrameBound{Type: "UNBOUNDED"}
	pos := window.ResolveFramePos(fb, 3, 10, true)
	if pos != 0 {
		t.Errorf("UNBOUNDED start = %d, want 0", pos)
	}
}

func TestResolveFramePos_Unbounded_End(t *testing.T) {
	fb := QP.FrameBound{Type: "UNBOUNDED"}
	pos := window.ResolveFramePos(fb, 3, 10, false)
	if pos != 9 {
		t.Errorf("UNBOUNDED end = %d, want 9", pos)
	}
}

func TestFrameBoundOffset_Nil(t *testing.T) {
	offset := window.FrameBoundOffset(nil)
	if offset != 0 {
		t.Errorf("FrameBoundOffset(nil) = %d, want 0", offset)
	}
}

func TestFrameBoundOffset_Int(t *testing.T) {
	offset := window.FrameBoundOffset(&QP.Literal{Value: int64(3)})
	if offset != 3 {
		t.Errorf("FrameBoundOffset(3) = %d, want 3", offset)
	}
}
