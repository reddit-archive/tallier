package tally

import (
	"testing"
)

func TestParseSample(t *testing.T) {
	_, err := ParseSample("test", "")
	if err == nil {
		t.Error("expected error")
	}
	_, err = ParseSample("test", "||")
	if err == nil {
		t.Error("expected error")
	}
	_, err = ParseSample("test", "x|")
	if err == nil {
		t.Error("expected error")
	}
	_, err = ParseSample("test", "1|x@y")
	if err == nil {
		t.Error("expected error")
	}

	expected := Sample{
		key:        "test",
		value:      3.5,
		valueType:  TIMER,
		sampleRate: 0.1,
	}
	sample, _ := ParseSample("test", "3.5|ms@0.1")
	if expected != sample {
		t.Errorf("expected %#v, got %#v", expected, sample)
	}

	expected.valueType = COUNTER
	sample, _ = ParseSample("test", "3.5|c@0.1")
	if expected != sample {
		t.Errorf("expected %#v, got %#v", expected, sample)
	}

	expected.sampleRate = 1.0
	sample, _ = ParseSample("test", "3.5|c")
	if expected != sample {
		t.Errorf("expected %#v, got %#v", expected, sample)
	}
}

func TestParseStatgramLine(t *testing.T) {
	statgram, err := ParseStatgramLine("")
	if err != nil {
		t.Error("expected empty statgram, got error:", err)
	}
	if len(statgram) > 0 {
		t.Errorf("expected empty statgram, got %#v", statgram)
	}

	statgram, _ = ParseStatgramLine("test")
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
	statgram, err = ParseStatgramLine("test:1|c:2|ms@0.1")
	if err != nil {
		t.Error("expected statgram, got error:", err)
	}
	if s, ok := assertDeepEqual(expected, statgram); !ok {
		t.Error(s)
	}

	statgram, err = ParseStatgramLine("test:0|s|x:0|s|a\\nb\\&c\\\\d\\;e:0|s|y")
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

	statgram, err = ParseStatgramLine("test:1|c:error")
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
	}
	statgram := ParseStatgram(
		"x:1|c:2|c\ny:1|ms@0.5:error\ns:0|s|a\\nb\\&c\\\\d\\;e\nz:0.1|c")
	if s, ok := assertDeepEqual(expected, statgram); !ok {
		t.Error(s)
	}

	statgram = ParseStatgram(
		"x:1|c\n^022|c\ny:1|ms@0.5:error\n^fferror\n" +
			"s:0|s|a\\nb\\&c\\\\d\\;e\nz:0.1|c")
	if s, ok := assertDeepEqual(expected, statgram); !ok {
		t.Error(s)
	}
}
