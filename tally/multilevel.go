package tally

import (
	"fmt"
	"time"
)

type CountBucket struct {
	value     float64
	timestamp time.Time
	next      *CountBucket
}

func (b *CountBucket) String() string {
	var next string
	if b.next != nil {
		next = "->" + b.next.String()
	}
	return fmt.Sprintf("%v%s", *b, next)
}

type CountLevel struct {
	Current     float64
	interval    time.Duration
	top, bottom *CountBucket
}

func (lvl *CountLevel) Count(value float64) {
	lvl.Current += value
	lvl.bottom.value += value
}

func (lvl *CountLevel) Duration() time.Duration {
	return time.Since(lvl.top.timestamp)
}

func (lvl *CountLevel) RatePer(unit time.Duration) float64 {
	return lvl.Current / (lvl.Duration().Seconds() / unit.Seconds())
}

func (lvl *CountLevel) NewBucket() {
	b := &CountBucket{0, time.Now(), nil}
	if lvl.top == nil {
		lvl.top = b
	} else {
		lvl.bottom.next = b
	}
	lvl.bottom = b
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
	var lastRemoved *CountBucket
	b := current.top
	for b != nil && now.Sub(b.timestamp) >= current.interval {
		total += b.value
		lastRemoved = b
		b = b.next
	}
	if b == nil {
		current.top = nil
		current.bottom = nil
	} else {
		current.top = b
	}
	if lastRemoved == nil {
		current.NewBucket()
	} else {
		lastRemoved.value = 0
		lastRemoved.next = nil
		if current.top == nil {
			current.top = lastRemoved
		} else {
			current.bottom.next = lastRemoved
		}
		current.bottom = lastRemoved
		current.Current -= total
		remainder.Rollup()
	}
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
		count[0].top.timestamp = time.Unix(0, 0)
	}
	return count
}
