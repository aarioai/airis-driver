package mysqli_test

import (
	"github.com/aarioai/airis-driver/driver/mysqli"
	"testing"
)

type stru struct {
	Name string `db:"name"`
	Age  int    `db:"age"`
}

func TestAnd(t *testing.T) {
	u := stru{
		Name: "Aario",
		Age:  18,
	}
	s := mysqli.And(u, "name", "age")
	s1 := "`age`=\"18\" AND `name`=\"Aario\""
	s2 := "`name`=\"Aario\" AND `age`=\"18\""
	if s != s1 && s != s2 {
		t.Errorf("sqlhelper.And(u, true, ...) == %s", s)
	}

	s = mysqli.And(u, "Name", "Age")
	if s != s1 && s != s2 {
		t.Errorf("sqlhelper.And(u, false, ...) == %s", s)
	}
}

func TestOr(t *testing.T) {
	u := stru{
		Name: "Aario",
		Age:  18,
	}
	s := mysqli.Or(u, "name", "age")
	s1 := "`age`=\"18\" OR `name`=\"Aario\""
	s2 := "`name`=\"Aario\" OR `age`=\"18\""
	if s != s1 && s != s2 {
		t.Errorf("sqlhelper.Or(u, true, ...) == %s", s)
	}

	s = mysqli.Or(u, "Name", "Age")
	if s != s1 && s != s2 {
		t.Errorf("sqlhelper.Or(u, false, ...) == %s", s)
	}
}

func TestAndWithWhere(t *testing.T) {
	u := stru{
		Name: "Aario",
		Age:  18,
	}
	s := mysqli.AndWithWhere(u, "name", "age")
	s1 := " WHERE `age`=\"18\" AND `name`=\"Aario\" "
	s2 := " WHERE `name`=\"Aario\" AND `age`=\"18\" "
	if s != s1 && s != s2 {
		t.Errorf("sqlhelper.Or(u, true, ...) == %s", s)
	}

	s = mysqli.AndWithWhere(u, "Name", "Age")
	if s != s1 && s != s2 {
		t.Errorf("sqlhelper.Or(u, false, ...) == %s", s)
	}
}

func TestOrWithWhere(t *testing.T) {
	u := stru{
		Name: "Aario",
		Age:  18,
	}
	s := mysqli.OrWithWhere(u, "name", "age")
	s1 := " WHERE `age`=\"18\" OR `name`=\"Aario\" "
	s2 := " WHERE `name`=\"Aario\" OR `age`=\"18\" "
	if s != s1 && s != s2 {
		t.Errorf("sqlhelper.Or(u, true, ...) == %s", s)
	}

	s = mysqli.OrWithWhere(u, "Name", "Age")
	if s != s1 && s != s2 {
		t.Errorf("sqlhelper.Or(u, false, ...) == %s", s)
	}
}
