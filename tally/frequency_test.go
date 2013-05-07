package tally

import (
	"testing"
	"time"
)

func fc(key string, value float64) FrequencyCount {
	c := make(MultilevelCount, 1)
	c[0].NewBucket()
	c[0].top.timestamp = time.Unix(0, 0)
	c.Count(value)
	return FrequencyCount{key, &c}
}

func TestSortedItems(t *testing.T) {
	fcr := NewFrequencyCounter(10)
	fcr.Count("x", 1)
	fcr.Count("y", 2)
	fcr.Count("z", 3)

	expected := FrequencyCountSlice{
		fc("z", 3),
		fc("y", 2),
		fc("x", 1),
	}
	result := fcr.SortedItems()
	if s, ok := assertDeepEqual(expected, result); !ok {
		t.Error(s)
	}
}

func TestTrim(t *testing.T) {
	fcr := NewFrequencyCounter(1)
	for count, key := range []string{"a", "b", "c", "d"} {
		fcr.Count(key, float64(count))
	}

	expected := FrequencyCountSlice{
		fc("d", 3),
		fc("c", 2),
	}
	fcr.Trim()
	result := fcr.SortedItems()
	if s, ok := assertDeepEqual(expected, result); !ok {
		t.Error(s)
	}

	for count, key := range []string{"a", "b", "c", "d"} {
		fcr.Count(key, float64(count))
	}
	expected = FrequencyCountSlice{
		fc("d", 6),
		fc("c", 4),
	}
	fcr.Trim()
	result = fcr.SortedItems()
	if s, ok := assertDeepEqual(expected, result); !ok {
		t.Error(s)
	}
}

func TestAggregate(t *testing.T) {
	child1 := NewFrequencyCounter(10)
	child1.Count("x", 1)
	child1.Count("y", 2)

	child2 := NewFrequencyCounter(10)
	child2.Count("x", 4)
	child2.Count("y", 5)
	child2.Count("z", 3)

	parent := NewFrequencyCounter(1)
	parent.Aggregate(child1)
	parent.Aggregate(child2)

	expected := FrequencyCountSlice{
		fc("y", 7),
		fc("x", 5),
		fc("z", 3),
	}
	result := parent.SortedItems()
	if s, ok := assertDeepEqual(expected, result); !ok {
		t.Error(s)
	}
}
