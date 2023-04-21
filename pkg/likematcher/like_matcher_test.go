package likematcher

import (
	"testing"
)

func Test(t *testing.T) {
	like := "%test%"
	escape := ""
	matcher, err := Compile(like, escape)

	if err != nil {
		t.Errorf("%v", err)
	}

	ss := []string{"test", "1test", "1test1", "tes"}
	for _, s := range ss {
		if matcher.Match([]byte(s)) {
			t.Logf("match %s", s)
		} else {
			t.Logf("does not match %s", s)
		}
	}
}
