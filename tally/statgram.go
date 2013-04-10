package tally

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type SampleType int

const (
	COUNTER SampleType = iota
	TIMER
	STRING
)

type Sample struct {
	key         string
	value       float64
	valueType   SampleType
	sampleRate  float64
	stringValue string
}

type Statgram []Sample

// ParseStatgram reads samples from the given text, returning a Statgram.
// The format of a statgram is line-oriented. Each line names a key and
// provides one or more sampled values for that key. The documentation for the
// ParseStatgramLine function explains the formatting of each line.
func ParseStatgram(text string) (statgram Statgram) {
	previous := "" // for decoding front compression

	lines := strings.Split(text, "\n")
	statgram = make(Statgram, 0, len(lines))
	total := 0
	for _, value := range lines {
		// check for front compression
		if len(value) > 2 && value[0] == '^' {
			prefixLength, err := strconv.ParseInt(value[1:3], 16, 0)
			if err == nil && int(prefixLength) < len(previous) {
				value = previous[:prefixLength] + value[3:]
			}
		}
		previous = value

		subsamples, _ := ParseStatgramLine(value)
		statgram = append(statgram, subsamples...)
		total += len(subsamples)
	}
	return
}

// ParseStatgramLine reads samples from one line of a statgram. This line
// provides a key name and one or more sampled values for that key. The key name
// and each of the values are separated by the ':' character. The format for
// each sampled value is explained in the documentation for ParseSample.
func ParseStatgramLine(text string) (statgram Statgram, err error) {
	parts := strings.Split(text, ":")
	if len(parts) == 0 {
		return
	}
	key := parts[0]
	statgram = make([]Sample, 0, len(parts)-1)
	for _, part := range parts[1:] {
		var sample Sample
		sample, err = ParseSample(key, part)
		if err != nil {
			return
		}
		statgram = append(statgram, sample)
	}
	return
}

// ParseSample decodes a formatted string encoding a sampled value. Sampled
// values are either counts or timings, and are also associated with a sample
// rate. The format is:
// <VALUE> '|' <TYPECODE> ['@' <SAMPLE_RATE>] ['|' <ENC_STRING>]
// The <VALUE> and optional <SAMPLE_RATE> tokens are floating point decimals. If
// the sample rate annotation isn't present, then it's assumed to be 1.0 (meaning
// 100%). The <TYPECODE> token is either 'c', 'ms', or 's', indicating a counter
// value, timer value, or string count respectively. In the case of a string
// count, the string being counted may be given via <ENC_STRING> (where special
// characters such as '\', '|', ':', and the newline are escaped).
func ParseSample(key string, part string) (sample Sample, err error) {
	fields := strings.Split(part, "|")
	if len(fields) < 2 || len(fields) > 3 {
		err = errors.New("sample field should contain one or two '|' separators")
		return
	}
	var value float64
	if value, err = strconv.ParseFloat(fields[0], 64); err != nil {
		return
	}
	sample = Sample{key: key, value: value, sampleRate: 1.0}
	if strings.Contains(fields[1], "@") {
		f1Parts := strings.SplitN(fields[1], "@", 2)
		fields[1] = f1Parts[0]
		sample.sampleRate, err = strconv.ParseFloat(f1Parts[1], 64)
		if err != nil {
			return
		}
	}
	switch fields[1] {
	case "c":
		sample.valueType = COUNTER
	case "ms":
		sample.valueType = TIMER
	case "s":
		if len(fields) < 3 {
			err = errors.New("string sample is missing its string")
			return
		}
		sample.valueType = STRING
		sample.stringValue = decodeStringSample(fields[2])
	default:
		err = errors.New(fmt.Sprintf("invalid sample type code %#v", fields[1]))
	}
	return
}

func decodeStringSample(encoded string) string {
	s := encoded
	s = strings.Replace(s, "\\n", "\n", -1)
	s = strings.Replace(s, "\\&", "|", -1)
	s = strings.Replace(s, "\\;", ":", -1)
	s = strings.Replace(s, "\\\\", "\\", -1)
	return s
}
