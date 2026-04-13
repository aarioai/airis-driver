package sqlx_test

import (
	"testing"

	"github.com/aarioai/airis-driver/driver/sqlx"
)

func TestInsertMany(t *testing.T) {
	wants := map[int]string{
		1: "INSERT INTO user (nickname, avatar, created_at, updated_at) VALUES (?, ?, now(), now())",
		2: "INSERT INTO user (nickname, avatar, created_at, updated_at) VALUES (?, ?, now(), now()),(?, ?, now(), now())",
	}

	for n, want := range wants {
		qs := sqlx.InsertMany("user", "(nickname, avatar, created_at, updated_at)", "(?, ?, now(), now())", n)
		if qs != want {
			t.Errorf("num=%d, qs = %q, want %q", n, qs, want)
		}
	}
}
