package tally

import (
	"testing"
)

func TestSortedItems(t *testing.T) {
	fcr := NewFrequencyCounter(10)
	fcr.Count("x", 1)
	fcr.Count("y", 2)
	fcr.Count("z", 3)

	expected := FrequencyCountSlice{
		FrequencyCount{"z", 3},
		FrequencyCount{"y", 2},
		FrequencyCount{"x", 1},
	}
	result := fcr.SortedItems()
	if !reflect.DeepEqual(expected, result) {
		t.Errorf("expected %#v, got %#v", expected, result)
	}
}

func TestTrim(t *testing.T) {
	fcr := NewFrequencyCounter(1)
	for count, key := range []string{"a", "b", "c", "d"} {
		fcr.Count(key, float64(count))
	}

	expected := FrequencyCountSlice{
		FrequencyCount{"d", 3},
		FrequencyCount{"c", 2},
	}
	fcr.Trim()
	result := fcr.SortedItems()
	if !reflect.DeepEqual(expected, result) {
		t.Errorf("expected %#v, got %#v", expected, result)
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
		FrequencyCount{"y", 7},
		FrequencyCount{"x", 5},
		FrequencyCount{"z", 3},
	}
	result := parent.SortedItems()
	if !reflect.DeepEqual(expected, result) {
		t.Errorf("expected %#v, got %#v", expected, result)
	}
}
