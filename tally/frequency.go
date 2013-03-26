package tally

import (
	"sort"
)

type FrequencyCount struct {
	key   string
	count float64
}

type FrequencyCountSlice []FrequencyCount

func (fcs FrequencyCountSlice) Len() int {
	return len(fcs)
}

func (fcs FrequencyCountSlice) Swap(i, j int) {
	fcs[i], fcs[j] = fcs[j], fcs[i]
}

func (fcs FrequencyCountSlice) Less(i, j int) bool {
	return fcs[j].count < fcs[i].count
}

type FrequencyCounter struct {
	capacity           int
	oversampleCapacity int
	totalObserved      float64
	frequencies        map[string]float64
}

func NewFrequencyCounter(capacity int) *FrequencyCounter {
	return &FrequencyCounter{capacity, capacity, 0, make(map[string]float64)}
}

func (fcr *FrequencyCounter) Count(key string, count float64) {
	fcr.totalObserved += count
	fcr.frequencies[key] += count
}

func (fcr *FrequencyCounter) Trim() {
	if len(fcr.frequencies) <= fcr.capacity+fcr.oversampleCapacity {
		return
	}
	items := fcr.SortedItems()
	for i := fcr.capacity + fcr.oversampleCapacity; i < len(items); i++ {
		delete(fcr.frequencies, items[i].key)
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
