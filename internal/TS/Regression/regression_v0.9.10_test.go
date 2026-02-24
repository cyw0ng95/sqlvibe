package Regression

import (
	"testing"
	"time"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestRegression_MalformedJoin_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	done := make(chan error, 1)
	go func() {
		_, err := db.Query("SELECT A FROM(SELECT,)t JOIN t JOIN")
		done <- err
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("query timed out - possible infinite loop")
	}
}
