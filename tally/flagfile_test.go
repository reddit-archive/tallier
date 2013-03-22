package tally

import (
	"bufio"
	"bytes"
	"io"
	"testing"
)

type TestLineReader [][]byte

func (r *TestLineReader) ReadLine() (line []byte, isPrefix bool, err error) {
	if len(*r) == 0 {
		err = io.EOF
		return
	}
	line = []byte{(*r)[0][0]}
	if len((*r)[0]) == 1 {
		*r = (*r)[1:]
	} else {
		isPrefix = true
		(*r)[0] = (*r)[0][1:]
	}
	return
}

func TestReadLine(t *testing.T) {
	r := &TestLineReader{[]byte("abc"), []byte("def")}
	expected := "abc"
	line, err := readLine(r)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if expected != line {
		t.Errorf("expected %#v, result was %#v", expected, line)
	}

	expected = "def"
	line, err = readLine(r)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if expected != line {
		t.Errorf("expected %#v, result was %#v", expected, line)
	}

	line, err = readLine(r)
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func TestReadArg(t *testing.T) {
	var b bytes.Buffer
	b.WriteString("\n")
	b.WriteString("   this is an error  \n")
	b.WriteString("   # this is a comment\n")
	b.WriteString("# this is another comment\n")
	b.WriteString("     this = flag with a value # and a comment\n")
	b.WriteString("\n")
	r := bufio.NewReader(&b)

	name, val, n, err := readArg(r)
	if err == nil {
		t.Error("expected error, got %#v, %#v, %#v", name, val, n)
	}
	if n != 2 {
		t.Errorf("expected lineno to be %d, got %d", 2, n)
	}

	name, val, n, err = readArg(r)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if n != 3 {
		t.Errorf("expected lineno to be %d, got %d", 3, n)
	}
	if name != "this" || val != "flag with a value" {
		t.Errorf("expected %v, %v; got %v, %v", "this", "flag with a value",
			name, val)
	}

	name, val, n, err = readArg(r)
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}
