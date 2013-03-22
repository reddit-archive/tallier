package tally

import (
	"bytes"
	"io"
	"reflect"
	"testing"
)

func TestReceiveOnce(t *testing.T) {
	receiver := new(Receiver)
	receiver.conn = bytes.NewBufferString("x:1|c")
	expected := Statgram{Sample{"x", 1.0, COUNTER, 1.0}}

	statgram, err := receiver.ReadOnce()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(expected, statgram) {
		t.Errorf("expected %v, result was %v", expected, statgram)
	}

	_, err = receiver.ReadOnce()
	if err != io.EOF {
		t.Errorf("expected EOF, got %v", err)
	}
}

func TestReceiveStatgrams(t *testing.T) {
	conn := new(bytes.Buffer)
	receiver := new(Receiver)
	receiver.conn = conn
	statgrams := receiver.ReceiveStatgrams()

	conn.Write([]byte("x:1.0|c"))
	expected := Statgram{Sample{"x", 1.0, COUNTER, 1.0}}
	statgram := <-statgrams
	if !reflect.DeepEqual(expected, statgram) {
		t.Errorf("expected %v, result was %v", expected, statgram)
	}

	conn.Write([]byte("y:2.0|ms@0.5"))
	expected = Statgram{Sample{"y", 2.0, TIMER, 0.5}}
}

type CoordinatedReader chan []byte

func (r *CoordinatedReader) Write(p []byte) (n int, e error) {
	*r <- p
	return len(p), nil
}

func (r *CoordinatedReader) Read(p []byte) (n int, err error) {
	b, ok := <-*r
	if ok {
		n = copy(p, b)
	} else {
		err = io.EOF
	}
	return
}

func (r *CoordinatedReader) Close() error {
	close(*r)
	return nil
}

func TestRunReceiver(t *testing.T) {
	expected := NewSnapshot()
	expected.Count("x", 3)
	expected.Count("tallier.messages.child_test", 2)
	expected.Count("tallier.bytes.child_test",
		float64(len("x:1.0|c")+len("x:2.0|c")))

	notifier := make(chan Statgram)
	conn := make(CoordinatedReader)
	control := RunReceiver("test", &conn, notifier)

	conn.Write([]byte("x:1.0|c"))
	conn.Write([]byte("x:2.0|c"))
	<-notifier
	<-notifier
	control <- nil
	snapshot := <-control
	if !reflect.DeepEqual(expected, snapshot) {
		t.Errorf("expected %v, result was %v", expected, snapshot)
	}
}
