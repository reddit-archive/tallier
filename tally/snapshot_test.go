package tally

import (
	"fmt"
	"runtime"
	"sort"
	"testing"
	"time"
)

func TestSnapshots(t *testing.T) {
	parent := NewSnapshot()
	a := NewSnapshot()
	b := NewSnapshot()
	a.ProcessStatgram(Statgram{
		Sample{"x", 1.0, COUNTER, 1.0, ""},
		Sample{"y", 1.0, COUNTER, 0.5, ""},
	})
	for i := 0.0; i < 10; i++ {
		a.Time("z", i)
	}
	a.Count("tallier.messages.child_1", 2)
	a.Count("tallier.bytes.child_1", 20)
	b.ProcessStatgram(Statgram{
		Sample{"y", 3.0, COUNTER, 1.0, ""},
		Sample{"z", 4.0, COUNTER, 1.0, ""},
	})
	for i := 0.0; i < 5; i++ {
		b.Time("z", 2*i)
	}
	b.Count("tallier.messages.child_2", 3)
	b.Count("tallier.bytes.child_2", 30)

	expected := NewSnapshot()
	expected.Count("x", 1)
	expected.Count("y", 5)
	expected.Count("z", 4)
	for i := 0; i < 10; i++ {
		if i%2 == 0 {
			expected.Time("z", float64(i))
		}
		expected.Time("z", float64(i))
	}
	expected.Count("tallier.messages.child_1", 2)
	expected.Count("tallier.bytes.child_1", 20)
	expected.Count("tallier.messages.child_2", 3)
	expected.Count("tallier.bytes.child_2", 30)
	expected.Count("tallier.messages.total", 5)
	expected.Count("tallier.bytes.total", 50)
	expected.CountString("tallier.samples", "x", 1)
	expected.CountString("tallier.samples", "y", 2)
	expected.CountString("tallier.samples", "z", 1)
	expected.numChildren = 2
	parent.Aggregate(a)
	parent.Aggregate(b)
	sort.Float64s(parent.timings["z"])
	if s, ok := assertDeepEqual(expected, parent); !ok {
		t.Error(s)
	}
}

func TestGraphiteReport(t *testing.T) {
	now := time.Now()
	timestamp := fmt.Sprintf(" %d\n", now.Unix())
	expected := []string{}

	snapshot := NewSnapshot()
	snapshot.start = now
	snapshot.duration = time.Duration(10) * time.Second
	report := snapshot.GraphiteReport()
	if s, ok := assertDeepEqual(expected, report); !ok {
		t.Error(s)
	}

	format := func(stat string, value float64) string {
		return fmt.Sprintf("%s %f%s", stat, value, timestamp)
	}
	expected = []string{
		format("stats.x", 10),
		format("stats_counts.x", 100),
		format("stats.timers.y.lower", 1),
		format("stats.timers.y.upper", 10),
		format("stats.timers.y.upper_90", 9),
		format("stats.timers.y.upper_99", 10),
		format("stats.timers.y.mean", 5.5),
		"stats.timers.y.count 10" + timestamp,
		format("stats.timers.y.rate", 1),
	}

	child := NewSnapshot()
	child.Count("x", 100)
	for i := 0.0; i < 10; i++ {
		child.Time("y", 10.0-i)
	}
	snapshot.Aggregate(child)
	report = snapshot.GraphiteReport()
	if s, ok := assertDeepEqual(expected, report); !ok {
		t.Error(s)
	}
}

func TestStringValues(t *testing.T) {
	statgram := Statgram{
		Sample{"x", 10, STRING, 1.0, "A"},
		Sample{"x", 1, STRING, 0.5, "B"},
	}
	snapshot := NewSnapshot()
	snapshot.ProcessStatgram(statgram)
	expected := FrequencyCountSlice{
		fc("A", 10),
		fc("B", 2),
	}
	result := snapshot.stringCounts["x"].SortedItems()
	if s, ok := assertDeepEqual(expected, result); !ok {
		t.Error(s)
	}
}

func TestStringValueAggregation(t *testing.T) {
	child1 := NewSnapshot()
	child1.CountString("x", "A", 1)
	child1.CountString("x", "B", 2)
	child1.CountString("y", "AA", 1)

	child2 := NewSnapshot()
	child2.CountString("x", "B", 2)
	child2.CountString("x", "C", 2)
	child2.CountString("z", "AAA", 1)

	expected := make(map[string]FrequencyCountSlice)
	expected["x"] = FrequencyCountSlice{
		fc("B", 4),
		fc("C", 2),
		fc("A", 1),
	}
	expected["y"] = FrequencyCountSlice{fc("AA", 1)}
	expected["z"] = FrequencyCountSlice{fc("AAA", 1)}
	parent := NewSnapshot()
	parent.Aggregate(child1)
	parent.Aggregate(child2)

	result := make(map[string]FrequencyCountSlice)
	for key, fcs := range parent.stringCounts {
		result[key] = fcs.SortedItems()
	}
	if s, ok := assertDeepEqual(expected, result); !ok {
		t.Error(s)
	}
}

func BenchmarkFlush(b *testing.B) {
	Y := 1000
	keys := make([]string, Y)
	for i := range keys {
		keys[i] = fmt.Sprintf("%d", i)
	}
	X := 10000
	snapshot := NewSnapshot()
	for i := 0; i < b.N; i++ {
		for j := 0; j < X; j++ {
			snapshot.Count(keys[i%Y], float64(j))
			snapshot.Time(keys[i%Y], float64(j))
		}
		snapshot.Flush()
		//runtime.GC()
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	b.Logf("run N=%d, pauses: %v", b.N, ms.PauseNs)
}
