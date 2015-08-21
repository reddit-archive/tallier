package tally

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
)

func TestParseSample(t *testing.T) {
	_, err := ParseSample("test", nil)
	if err == nil {
		t.Error("expected error")
	}
	_, err = ParseSample("test", []byte("||"))
	if err == nil {
		t.Error("expected error")
	}
	_, err = ParseSample("test", []byte("x|"))
	if err == nil {
		t.Error("expected error")
	}
	_, err = ParseSample("test", []byte("1|x@y"))
	if err == nil {
		t.Error("expected error")
	}

	expected := Sample{
		key:        "test",
		value:      3.5,
		valueType:  TIMER,
		sampleRate: 0.1,
	}
	sample, _ := ParseSample("test", []byte("3.5|ms@0.1"))
	if expected != sample {
		t.Errorf("expected %#v, got %#v", expected, sample)
	}

	expected.valueType = COUNTER
	sample, _ = ParseSample("test", []byte("3.5|c@0.1"))
	if expected != sample {
		t.Errorf("expected %#v, got %#v", expected, sample)
	}

	expected.sampleRate = 1.0
	sample, _ = ParseSample("test", []byte("3.5|c"))
	if expected != sample {
		t.Errorf("expected %#v, got %#v", expected, sample)
	}

	expected = Sample{
		key:        "test",
		valueType:  GAUGE,
		value:      120,
		sampleRate: 1.0,
	}

	sample, _ = ParseSample("test", []byte("+120|g"))
	if expected != sample {
		t.Errorf("expected %#v, got %#v", expected, sample)
	}
}

func TestParseStatgramLine(t *testing.T) {
	parser := NewStatgramParser()
	statgram, err := parser.ParseStatgramLine(nil)
	if err != nil {
		t.Error("expected empty statgram, got error:", err)
	}
	if len(statgram) > 0 {
		t.Errorf("expected empty statgram, got %#v", statgram)
	}

	statgram, err = parser.ParseStatgramLine([]byte("test"))
	if err != nil {
		t.Error("expected empty statgram, got error:", err)
	}
	if len(statgram) > 0 {
		t.Errorf("expected empty statgram, got %#v", statgram)
	}

	expected := Statgram{
		Sample{key: "test", value: 1.0, valueType: COUNTER, sampleRate: 1.0},
		Sample{key: "test", value: 2.0, valueType: TIMER, sampleRate: 0.1},
	}
	statgram, err = parser.ParseStatgramLine([]byte("test:1|c:2|ms@0.1"))
	if err != nil {
		t.Error("expected statgram, got error:", err)
	}
	if s, ok := assertDeepEqual(expected, statgram); !ok {
		t.Error(s)
	}

	expected = Statgram{
		Sample{key: "test", value: -5, valueType: GAUGE, sampleRate: 1.0},
	}
	statgram, err = parser.ParseStatgramLine([]byte("test:-5|g\n"))
	if err != nil {
		t.Error("expected statgram, got error:", err)
	}
	if s, ok := assertDeepEqual(expected, statgram); !ok {
		t.Error(s)
	}

	statgram, err = parser.ParseStatgramLine(
		[]byte("test:0|s|x:0|s|a\\nb\\&c\\\\d\\;e:0|s|y"))
	expected = Statgram{
		Sample{key: "test", valueType: STRING, sampleRate: 1.0,
			stringValue: "x"},
		Sample{key: "test", valueType: STRING, sampleRate: 1.0,
			stringValue: "a\nb|c\\d:e"},
		Sample{key: "test", valueType: STRING, sampleRate: 1.0,
			stringValue: "y"},
	}
	if err != nil {
		t.Error("expected statgram, got error:", err)
	}
	if s, ok := assertDeepEqual(expected, statgram); !ok {
		t.Error(s)
	}

	_, err = parser.ParseStatgramLine([]byte("test:1|c:error"))
	if err == nil {
		t.Error("expected error, got:", statgram)
	}
}

func TestParseStatgram(t *testing.T) {
	expected := Statgram{
		Sample{key: "x", value: 1.0, valueType: COUNTER, sampleRate: 1.0},
		Sample{key: "x", value: 2.0, valueType: COUNTER, sampleRate: 1.0},
		Sample{key: "y", value: 1.0, valueType: TIMER, sampleRate: 0.5},
		Sample{key: "s", valueType: STRING, stringValue: "a\nb|c\\d:e",
			sampleRate: 1.0},
		Sample{key: "z", value: 0.1, valueType: COUNTER, sampleRate: 1.0},
		Sample{key: "w", value: 120, valueType: GAUGE, sampleRate: 1.0, replace: true},
	}
	parser := NewStatgramParser()
	statgram := parser.ParseStatgram(
		[]byte("x:1|c:2|c\ny:1|ms@0.5:err\ns:0|s|a\\nb\\&c\\\\d\\;e\nz:0.1|c\nw:120|g\n"))
	if s, ok := assertDeepEqual(expected, statgram); !ok {
		t.Error(s)
	}

	statgram = parser.ParseStatgram(
		[]byte("x:1|c\n^022|c\ny:1|ms@0.5:error\n^fferror\n" +
			"s:0|s|a\\nb\\&c\\\\d\\;e\nz:0.1|c\nw:120|g\n"))
	if s, ok := assertDeepEqual(expected, statgram); !ok {
		t.Error(s)
	}
}

func TestLongCompressedStatgramLine(t *testing.T) {
	expected := Statgram{
		Sample{key: strings.Repeat("a", 255), value: 1.0, valueType: COUNTER, sampleRate: 1.0},
		Sample{key: "test", value: 1.0, valueType: COUNTER, sampleRate: 1.0},
	}

	parser := NewStatgramParser()
	// 255 "a"s plus 769 "b"s is 1024 which will take us over the limit
	statgram := parser.ParseStatgram(
		[]byte(strings.Repeat("a", 255) + ":1|c\n^ff" + strings.Repeat("b", 769) + ":1|c\n^02bad:1|c\ntest:1|c"))

	if s, ok := assertDeepEqual(expected, statgram); !ok {
		t.Error(s)
	}
}

func BenchmarkParseStatgram(b *testing.B) {
	bs := []byte("x:1|c:2|c\ny:1|m@0.5:e\ns:0|s|a\\nb\\&c\\\\d\\;e\nz:0.1|c\nl:+100|g")
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	s := ms.HeapObjects
	parser := NewStatgramParser()
	for i := 0; i < b.N; i++ {
		parser.ParseStatgram(bs)
	}
	runtime.GC()
	runtime.ReadMemStats(&ms)
	fmt.Fprintf(os.Stderr, "N=%d, heap objects: %d\n", b.N, ms.HeapObjects-s)
}
