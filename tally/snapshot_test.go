package tally

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

func TestSnapshots(t *testing.T) {
	parent := NewSnapshot()
	a := NewSnapshot()
	b := NewSnapshot()
	a.ProcessStatgram(Statgram{
		Sample{"x", 1.0, COUNTER, 1.0},
		Sample{"y", 1.0, COUNTER, 0.5},
	})
	for i := 0.0; i < 10; i++ {
		a.Time("z", i)
	}
	a.Count("tallier.messages.child_1", 2)
	a.Count("tallier.bytes.child_1", 20)
	b.ProcessStatgram(Statgram{
		Sample{"y", 3.0, COUNTER, 1.0},
		Sample{"z", 4.0, COUNTER, 1.0},
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
	expected := []string{
		"stats.tallier.num_stats 0" + timestamp,
		"stats.tallier.num_workers 0" + timestamp,
	}

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
		format("stats.timers.y.mean", 5.5),
		"stats.timers.y.count 10" + timestamp,
		format("stats.timers.y.rate", 1),
		"stats.tallier.num_stats 2" + timestamp,
		"stats.tallier.num_workers 1" + timestamp,
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
