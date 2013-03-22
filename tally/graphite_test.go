package tally

import (
	"bytes"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
)

type bufDialer struct{ buffer bytes.Buffer }
type failDialer struct{}

func (dialer *bufDialer) Dial(*net.TCPAddr) (io.WriteCloser, error) {
	return dialer, nil
}

func (dialer *bufDialer) Write(b []byte) (int, error) {
	return dialer.buffer.Write(b)
}

func (*bufDialer) Close() error {
	return nil
}

func (failDialer) Dial(*net.TCPAddr) (io.WriteCloser, error) {
	return nil, errors.New("this dialer always fails")
}

func testSnapshot() *Snapshot {
	snapshot := NewSnapshot()
	snapshot.Count("x", 1)
	snapshot.Time("y", 2)
	return snapshot
}

func TestSendReportErrorHandling(t *testing.T) {
	var dialer failDialer
	graphite, err := NewGraphite("localhost:7", dialer)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	err = graphite.SendReport(testSnapshot())
	if err == nil {
		t.Error("error expected!")
	}
}

func TestSendReport(t *testing.T) {
	dialer := new(bufDialer)
	graphite, err := NewGraphite("localhost:7", dialer)
	snapshot := testSnapshot()
	if err == nil {
		err = graphite.SendReport(snapshot)
	}
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	expected := strings.Join(snapshot.GraphiteReport(), "")
	sent := dialer.buffer.String()
	if expected != sent {
		t.Errorf("  expected:%v\n  but this was sent:\n%v", expected, sent)
	}
}
