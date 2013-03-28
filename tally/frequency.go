package tally

import (
	"sort"
	"time"
)

type FrequencyCount struct {
	key   string
	count MultilevelCount
}

type FrequencyCountSlice []FrequencyCount

func (fcs FrequencyCountSlice) Len() int {
	return len(fcs)
}

func (fcs FrequencyCountSlice) Swap(i, j int) {
	fcs[i], fcs[j] = fcs[j], fcs[i]
}

func (fcs FrequencyCountSlice) Less(i, j int) bool {
	return fcs[j].count.Total() < fcs[i].count.Total()
}

type FrequencyCounter struct {
	capacity           int
	intervals          []time.Duration
	oversampleCapacity int
	totalObserved      float64
	frequencies        map[string]MultilevelCount
}

func NewFrequencyCounter(capacity int,
	intervals ...time.Duration) *FrequencyCounter {
	return &FrequencyCounter{
		capacity:           capacity,
		intervals:          intervals,
		oversampleCapacity: capacity,
		frequencies:        make(map[string]MultilevelCount),
	}
}

func (fcr *FrequencyCounter) Count(key string, count float64) {
	fcr.totalObserved += count
	mc, ok := fcr.frequencies[key]
	if !ok {
		fcr.frequencies[key] = NewMultilevelCount(fcr.intervals...)
		mc = fcr.frequencies[key]
	}
	mc.Count(count)
}

func (fcr *FrequencyCounter) Trim() {
	items := fcr.SortedItems()
	for i, item := range items {
		if i < fcr.capacity+fcr.oversampleCapacity {
			fcr.frequencies[item.key].Rollup()
		} else {
			delete(fcr.frequencies, items[i].key)
		}
	}
}

func (fcr *FrequencyCounter) SortedItems() FrequencyCountSlice {
	fcs := make(FrequencyCountSlice, 0, len(fcr.frequencies))
	for key, value := range fcr.frequencies {
		fcs = append(fcs, FrequencyCount{key, value})
	}
	sort.Sort(fcs)
	return fcs
}

func (fcr *FrequencyCounter) Aggregate(child *FrequencyCounter) {
	for key, count := range child.frequencies {
		fcr.Count(key, count.Total())
	}
}
