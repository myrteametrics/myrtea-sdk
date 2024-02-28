package expression

import "testing"

// Usage: <string> <old> <new>
func TestReplace(t *testing.T) {
	// function needs exactly 3 parameters
	val, err := replace()
	if err == nil {
		t.Error(err)
		t.FailNow()
	}

	val, err = replace("test", "s", "b")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	AssertEqual(t, val, "tebt", "invalid replacement")

	val, err = replace("Hello World!", "World", "Myrtea")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	AssertEqual(t, val, "Hello Myrtea!", "invalid replacement")

}

func TestAtoi(t *testing.T) {

	val, err := replace()
	if err == nil {
		t.Error(err)
		t.FailNow()
	}

	val, err = replace("*ER*", "*ER*", "-100")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	AssertEqual(t, val, "-100", "invalid replacement")

	val, err = atoi(val)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	AssertEqual(t, val, -100, "invalid replacement")

}
