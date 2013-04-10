package tally

import (
	"time"
)

type CountLevel struct {
	Current    float64
	interval   time.Duration
	buckets    []float64
	timestamps []time.Time
}

func (lvl *CountLevel) Count(value float64) {
	lvl.Current += value
	lvl.buckets[len(lvl.buckets)-1] += value
}

func (lvl *CountLevel) Duration() time.Duration {
	return time.Since(lvl.timestamps[0])
}

func (lvl *CountLevel) RatePer(unit time.Duration) float64 {
	return lvl.Current / (lvl.Duration().Seconds() / unit.Seconds())
}

func (lvl *CountLevel) NewBucket() {
	lvl.buckets = append(lvl.buckets, 0)
	lvl.timestamps = append(lvl.timestamps, time.Now())
}

type MultilevelCount []CountLevel

func (mc MultilevelCount) Count(value float64) {
	for i, _ := range mc {
		mc[i].Count(value)
	}
}

func (mc MultilevelCount) Total() float64 {
	return mc[len(mc)-1].Current
}

func (mc MultilevelCount) Rollup() {
	if len(mc) == 0 {
		return
	}
	current := &mc[0]
	if current.interval == time.Duration(0) && len(mc) < 2 {
		// special case to simplify testing, doesn't change functionality
		return
	}
	remainder := mc[1:]
	now := time.Now()
	total := 0.0
	i := 0
	for i < len(current.timestamps) && now.Sub(current.timestamps[i]) >= current.interval {
		total += current.buckets[i]
		i++
	}
	if i > 0 {
		current.buckets = current.buckets[i:]
		current.timestamps = current.timestamps[i:]
		current.Current -= total
		remainder.Rollup()
	}
	current.NewBucket()
}

func NewMultilevelCount(intervals ...time.Duration) MultilevelCount {
	count := make(MultilevelCount, len(intervals)+1)
	count[0].NewBucket()
	for i, interval := range intervals {
		count[i+1].interval = interval
		count[i+1].NewBucket()
	}
	if len(intervals) == 0 {
		// special case to simplify testing, doesn't change functionality
		count[0].timestamps[0] = time.Unix(0, 0)
	}
	return count
}
