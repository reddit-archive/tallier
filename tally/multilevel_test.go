package tally

import (
	"testing"
	"time"
)

func TestCount(t *testing.T) {
	counter := *NewMultilevelCount(time.Minute, time.Hour)
	counter.Count(1)
	for i := 0; i < len(counter); i++ {
		if counter[i].Current != 1 {
			t.Errorf("expected count at level %d to be %v, got %v",
				i, 1.0, counter[i].Current)
		}
	}
}

func TestRollup(t *testing.T) {
	counter := *NewMultilevelCount(time.Minute, time.Hour,
		time.Duration(24)*time.Hour)
	for i := 0; i < 3; i++ {
		counter[i].top.timestamp = time.Now().Add(-counter[i].interval)
	}
	counter.Count(1)
	counter.Rollup()

	for i := 0; i < 3; i++ {
		if counter[i].Current != 0 {
			t.Errorf("expected count at level %d to be %v, got %v",
				i, 0.0, counter[i].Current)
		}
		if counter[i].top.next != nil {
			t.Errorf("expected only one bucket at level %d", i)
		}
	}
	if counter[3].top.next != counter[3].bottom {
		t.Errorf("expected two buckets at level 3, buckets are: %s",
			counter[3].top)
	}
	if counter[3].Current != 1 {
		t.Errorf("expected count at level 3 to be %v, got %v", 1.0,
			counter[3].Current)
	}

	counter.Count(2)
	if counter[3].Current != 3 {
		t.Errorf("expected count at level 3 to be %v, got %v", 3.0,
			counter[3].Current)
	}
	// force rollup on all levels, and make the final level's first bucket
	// expire
	for i := 0; i < len(counter); i++ {
		counter[i].top.timestamp = time.Now().Add(-counter[i].interval)
	}
	counter.Rollup()
	for i := 0; i < 3; i++ {
		if counter[i].Current != 0 {
			t.Errorf("expected count at level %d to be %v, got %v",
				i, 0.0, counter[i].Current)
		}
	}
	if counter[3].Current != 2 {
		t.Errorf("expected count at level 3 to be %v, got %v", 2.0,
			counter[3].Current)
	}
}

func BenchmarkRollupMemory(b *testing.B) {
	counter := NewMultilevelCount(
		time.Duration(100000),
		time.Duration(200000),
		time.Duration(400000))
	for i := 0; i < b.N; i++ {
		counter.Rollup()
	}
}
