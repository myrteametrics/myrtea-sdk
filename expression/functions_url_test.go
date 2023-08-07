package expression

import "testing"

// Usage: <string>
func TestUrlEncode(t *testing.T) {
	// function needs exactly 1 parameter
	val, err := urlEncode()
	if err == nil {
		t.Error(err)
		t.FailNow()
	}

	val, err = urlEncode("test", "s", "b")
	if err == nil {
		t.Error(err)
		t.FailNow()
	}

	val, err = urlEncode("2023-08-03 07:23:00")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	AssertEqual(t, val, "2023-08-03+07%3A23%3A00", "invalid url encode")
}

// Usage: <string>
func TestUrlDecode(t *testing.T) {
	// function needs exactly 1 parameter
	val, err := urlDecode()
	if err == nil {
		t.Error(err)
		t.FailNow()
	}

	val, err = urlDecode("test", "s", "b")
	if err == nil {
		t.Error(err)
		t.FailNow()
	}

	val, err = urlDecode("2023-08-03 07:23:00")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	AssertEqual(t, val, "2023-08-03 07:23:00", "invalid url decode")

	val, err = urlDecode("2023-08-03+07%3A23%3A00")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	AssertEqual(t, val, "2023-08-03 07:23:00", "invalid url decode")
}
