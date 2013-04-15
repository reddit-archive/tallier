package tally

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

const TIMINGS_INITIAL_CAPACITY = 1024
const STRING_COUNT_CAPACITY = 1024

type ReportedValue struct {
	value     float64
	timestamp time.Time
}

type Snapshot struct {
	reports              map[string]ReportedValue
	counts               map[string]float64
	timings              map[string][]float64
	stringCounts         map[string]*FrequencyCounter
	stringCountIntervals []time.Duration
	start                time.Time
	duration             time.Duration
	numChildren          int
}

func NewSnapshot() *Snapshot {
	return &Snapshot{
		reports:      make(map[string]ReportedValue),
		counts:       make(map[string]float64),
		timings:      make(map[string][]float64),
		stringCounts: make(map[string]*FrequencyCounter),
		numChildren:  0,
	}
}

func (snapshot *Snapshot) NumStats() int {
	return len(snapshot.counts) + len(snapshot.timings)
}

func (snapshot *Snapshot) Count(key string, value float64) {
	snapshot.counts[key] += value
}

func (snapshot *Snapshot) Time(key string, value float64) {
	var timings []float64
	var present bool
	if timings, present = snapshot.timings[key]; !present {
		timings = make([]float64, 0, TIMINGS_INITIAL_CAPACITY)
	}
	snapshot.timings[key] = append(timings, value)
}

func (snapshot *Snapshot) CountString(key, value string, count float64) {
	fc, ok := snapshot.stringCounts[key]
	if !ok {
		fc = NewFrequencyCounter(STRING_COUNT_CAPACITY,
			snapshot.stringCountIntervals...)
		snapshot.stringCounts[key] = fc
	}
	fc.Count(value, count)
}

func (snapshot *Snapshot) Report(key string, value float64, ts ...time.Time) {
	var t time.Time
	if len(ts) == 0 {
		t = time.Now()
	} else {
		t = ts[0]
	}
	snapshot.reports[key] = ReportedValue{value, t}
}

// ProcessStatgram accumulates a statistic report into the current snapshot.
func (snapshot *Snapshot) ProcessStatgram(statgram Statgram) {
	for _, sample := range statgram {
		switch sample.valueType {
		case COUNTER:
			snapshot.Count(sample.key, sample.value/sample.sampleRate)
		case TIMER:
			snapshot.Time(sample.key, sample.value)
		case STRING:
			snapshot.CountString(sample.key, sample.stringValue,
				sample.value/sample.sampleRate)
		}
		snapshot.CountString("tallier.samples", sample.key, 1)
	}
}

func (snapshot *Snapshot) Aggregate(child *Snapshot) {
	for key, value := range child.counts {
		snapshot.Count(key, value)
		if strings.HasPrefix(key, "tallier.messages.child_") {
			snapshot.Count("tallier.messages.total", value)
		} else if strings.HasPrefix(key, "tallier.bytes.child_") {
			snapshot.Count("tallier.bytes.total", value)
		}
	}
	for key, timings := range child.timings {
		snapshot.timings[key] = append(snapshot.timings[key], timings...)
	}
	for key, stringCounts := range child.stringCounts {
		fc, ok := snapshot.stringCounts[key]
		if !ok {
			fc = NewFrequencyCounter(STRING_COUNT_CAPACITY,
				snapshot.stringCountIntervals...)
			snapshot.stringCounts[key] = fc
		}
		fc.Aggregate(stringCounts)
	}
	snapshot.numChildren++
}

func (snapshot *Snapshot) GraphiteReport() (report []string) {
	timestamp := fmt.Sprintf(" %d\n", snapshot.start.Unix())
	makeLine := func(format string, params ...interface{}) string {
		return fmt.Sprintf(format, params...) + timestamp
	}
	report = make([]string, 0, 2*len(snapshot.counts)+6*
		len(snapshot.timings)+len(snapshot.reports)+2)
	counterScale := 1.0 / snapshot.duration.Seconds()
	for key, value := range snapshot.counts {
		report = append(report, makeLine("stats.%s %f", key, value*counterScale))
		report = append(report, makeLine("stats_counts.%s %f", key, value))
	}
	for key, timings := range snapshot.timings {
		if len(timings) == 0 {
			continue
		}
		sum := 0.0
		for _, value := range timings {
			sum += value
		}
		sort.Float64s(timings)
		report = append(report, makeLine("stats.timers.%s.lower %f", key,
			timings[0]))
		report = append(report, makeLine("stats.timers.%s.upper %f", key,
			timings[len(timings)-1]))
		report = append(report, makeLine("stats.timers.%s.upper_90 %f", key,
			timings[int(math.Ceil(0.9*float64(len(timings)))-1)]))
		report = append(report, makeLine("stats.timers.%s.mean %f", key,
			sum/float64(len(timings))))
		report = append(report, makeLine("stats.timers.%s.count %d", key,
			len(timings)))
		report = append(report, makeLine("stats.timers.%s.rate %f", key,
			float64(len(timings))/snapshot.duration.Seconds()))
	}
	for key, rvalue := range snapshot.reports {
		report = append(report, fmt.Sprintf("stats.%s %f %d\n", key,
			rvalue.value, rvalue.timestamp.Unix()))
	}
	return
}

func (snapshot *Snapshot) Flush() {
	snapshot.reports = make(map[string]ReportedValue, len(snapshot.reports))
	snapshot.counts = make(map[string]float64, len(snapshot.counts))
	snapshot.timings = make(map[string][]float64, len(snapshot.timings))
	for _, fcs := range snapshot.stringCounts {
		fcs.Trim()
	}
}
