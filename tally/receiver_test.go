package tally

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"testing"
)

func TestReceiveOnce(t *testing.T) {
	receiver := NewReceiver()
	receiver.conn = bytes.NewBufferString("x:1|c")
	expected := Statgram{Sample{"x", 1.0, COUNTER, 1.0, "", false}}

	statgram, err := receiver.ReadOnce()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if s, ok := assertDeepEqual(expected, statgram); !ok {
		t.Error(s)
	}
	if _, err = receiver.ReadOnce(); err != io.EOF {
		t.Errorf("expected EOF, got %v", err)
	}
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

func TestReceiveStatgrams(t *testing.T) {
	conn := make(CoordinatedReader)
	receiver := NewReceiver()
	receiver.conn = &conn
	statgrams := receiver.ReceiveStatgrams()

	conn.Write([]byte("x:1.0|c"))
	expected := Statgram{Sample{"x", 1.0, COUNTER, 1.0, "", false}}
	statgram := <-statgrams
	if s, ok := assertDeepEqual(expected, statgram); !ok {
		t.Error(s)
	}

	conn.Write([]byte("y:2.0|ms@0.5"))
	expected = Statgram{Sample{"y", 2.0, TIMER, 0.5, "", false}}
	statgram = <-statgrams
	if s, ok := assertDeepEqual(expected, statgram); !ok {
		t.Error(s)
	}

	conn.Write([]byte("test:10|g"))
	expected = Statgram{Sample{"test", 10, GAUGE, 1, "", true}}
	statgram = <-statgrams
	if s, ok := assertDeepEqual(expected, statgram); !ok {
		t.Error(s)
	}
}

func TestRunReceiver(t *testing.T) {
	expected := NewSnapshot()
	expected.Count("x", 3)
	expected.Count("tallier.messages.child_test", 2)
	expected.Count("tallier.bytes.child_test",
		float64(len("x:1.0|c")+len("x:2.0|c")))
	expected.CountString("tallier.samples", "x", 2)

	notifier := make(chan Statgram)
	conn := make(CoordinatedReader)
	control := RunReceiver("test", &conn, notifier)

	conn.Write([]byte("x:1.0|c"))
	conn.Write([]byte("x:2.0|c"))
	<-notifier
	<-notifier
	control <- nil
	snapshot := <-control
	if s, ok := assertDeepEqual(expected, snapshot); !ok {
		t.Error(s)
	}
}

func BenchmarkRunReceiver(b *testing.B) {
	bs := []byte("x:1|c:2|c\ny:1|m@0.5:e\ns:0|s|a\\nb\\&c\\\\d\\;e\nz:0.1|c")
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	s := ms.HeapObjects
	conn := make(CoordinatedReader)
	notifier := make(chan Statgram)
	RunReceiver("test", &conn, notifier)
	for i := 0; i < b.N; i++ {
		conn.Write(bs)
		<-notifier
	}
	runtime.GC()
	runtime.ReadMemStats(&ms)
	fmt.Fprintf(os.Stderr, "N=%d, heap objects: %d\n", b.N, ms.HeapObjects-s)
}
