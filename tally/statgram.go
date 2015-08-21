package tally

// #include <stdlib.h>
import "C"

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unsafe"
)

type SampleType int

const (
	COUNTER SampleType = iota
	TIMER
	STRING
	GAUGE
)

const MAX_LINE_LEN = 1024

type Sample struct {
	key         string
	value       float64
	valueType   SampleType
	sampleRate  float64
	stringValue string
	replace     bool
}

type Statgram []Sample

type StatgramParser struct {
	Statgram
	Length         int
	previousBuffer []byte
}

func NewStatgramParser() *StatgramParser {
	return &StatgramParser{make(Statgram, 1024), 0, make([]byte, MAX_LINE_LEN)}
}

// ParseStatgram reads samples from the given text, returning a Statgram.
// The format of a statgram is line-oriented. Each line names a key and
// provides one or more sampled values for that key. The documentation for the
// ParseStatgramLine function explains the formatting of each line.
func (parser *StatgramParser) ParseStatgram(datagram []byte) Statgram {
	parser.Length = 0
	previousLen := 0
	for i := 0; i < len(datagram); i++ {
		j := bytes.IndexByte(datagram[i:], '\n')
		if j == -1 {
			j = len(datagram)
		} else {
			j += i
		}
		line := datagram[i:j]
		i = j
		if len(line) > 2 && line[0] == '^' {
			prefixLen, err := strconv.ParseInt(string(line[1:3]), 16, 0)
			if err == nil && int(prefixLen) <= previousLen {
				lineLength := int(prefixLen) + len(line) - 3
				if lineLength <= MAX_LINE_LEN {
					copy(parser.previousBuffer[prefixLen:], line[3:])
					line = parser.previousBuffer[:lineLength]
				} else {
					line = nil
				}
			} else {
				line = nil
			}
		} else {
			copy(parser.previousBuffer, line)
		}

		if line != nil {
			parser.ParseStatgramLine(line)
			previousLen = len(line)
		} else {
			previousLen = 0
		}
	}
	return parser.Statgram[:parser.Length]
}

// ParseStatgramLine reads samples from one line of a statgram. This line
// provides a key name and one or more sampled values for that key. The key name
// and each of the values are separated by the ':' character. The format for
// each sampled value is explained in the documentation for ParseSample.
func (parser *StatgramParser) ParseStatgramLine(line []byte) (s Statgram,
	err error) {
	start := parser.Length
	var i int
	for ; i < len(line) && line[i] != ':'; i++ {
	}
	if i == len(line) {
		return
	}
	key := string(line[:i])
	remainder := line[i+1:]
	for len(remainder) > 0 {
		part := remainder
		i = bytes.IndexByte(part, ':')
		if i >= 0 {
			remainder = part[i+1:]
			part = part[:i]
		} else {
			remainder = nil
		}
		var sample Sample
		sample, err = ParseSample(key, part)
		if err != nil {
			return
		}
		if parser.Length >= len(parser.Statgram) {
			parser.Statgram = append(parser.Statgram, sample)
		} else {
			parser.Statgram[parser.Length] = sample
		}
		parser.Length++
	}
	s = parser.Statgram[start:parser.Length]
	return
}

// ParseSample decodes a formatted string encoding a sampled value. Sampled
// values are either counts or timings, and are also associated with a sample
// rate. The format is:
// <VALUE> '|' <TYPECODE> ['@' <SAMPLE_RATE>] ['|' <ENC_STRING>]
// The <VALUE> and optional <SAMPLE_RATE> tokens are floating point decimals. If
// the sample rate annotation isn't present, then it's assumed to be 1.0 (100%).
// The <TYPECODE> token is either 'c', 'ms', or 's', indicating a counter value,
// timer value, or string count respectively. In the case of a string count, the
// string being counted may be given via <ENC_STRING> (where special characters
// such as '\', '|', ':', and the newline are escaped).
func ParseSample(key string, part []byte) (sample Sample, err error) {
	i := bytes.IndexByte(part, '|')
	if i < 0 {
		err = errors.New("sample field should contain one or two '|' separators")
		return
	}
	var value float64
	part[i] = 0
	if value, err = ParseFloat(part); err != nil {
		return
	}
	remainder := part[i+1:]
	typeCode := remainder
	var suffix []byte
	i = bytes.IndexByte(remainder, '|')
	if i >= 0 {
		typeCode = remainder[:i]
		suffix = remainder[i+1:]
	}
	if len(typeCode) == 0 {
		err = errors.New("sample type code missing")
		return
	}
	sample = Sample{key: key, value: value, sampleRate: 1.0}
	if j := bytes.IndexByte(typeCode, '@'); j >= 0 {
		copy(typeCode[j:], typeCode[j+1:])
		typeCode[len(typeCode)-1] = 0
		sample.sampleRate, err = ParseFloat(typeCode[j:])
		if err != nil {
			return
		}
		typeCode = typeCode[:j]
	}
	switch typeCode[0] {
	case 'c':
		sample.valueType = COUNTER
	case 'm':
		sample.valueType = TIMER
	case 's':
		sample.valueType = STRING
		sample.stringValue = decodeStringSample(suffix)
	case 'g':
		sample.valueType = GAUGE
		sample.value, sample.replace = decodeGaugeSample(bytes.TrimLeft(part, "|"))
	default:
		err = errors.New(fmt.Sprintf("invalid sample type code %#v", typeCode))
	}
	return
}

var stringSampleReplacer *strings.Replacer

func decodeGaugeSample(sample []byte) (value float64, replace bool) {
	replace = false
	value, _ = ParseFloat(sample)

	if _, err := strconv.Atoi(string(sample[0])); err == nil {
		replace = true
	}

	return
}

// Decodes a string sample in-place.
func decodeStringSample(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	var i, j int
	for ; i < len(b); i++ {
		if b[i] == '\\' && i < len(b)-1 {
			i++
			switch b[i] {
			case '&':
				b[j] = '|'
			case ';':
				b[j] = ':'
			case 'n':
				b[j] = '\n'
			case '\\':
				b[j] = '\\'
			default:
				b[j] = b[i-1]
				j++
				b[j] = b[i]
			}
		} else {
			b[j] = b[i]
		}
		j++
	}
	return string(b[:j])
}

// b must be NUL-terminated
func ParseFloat(b []byte) (f float64, e error) {
	cbuf := (*C.char)(unsafe.Pointer(&b[0]))
	endptr := (*C.char)(nil)
	f = float64(C.strtod(cbuf, &endptr))
	if endptr == cbuf {
		e = errors.New("error parsing float")
	}
	return
}
