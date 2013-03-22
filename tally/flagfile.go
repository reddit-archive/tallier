package tally

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

type LineReader interface {
	ReadLine() ([]byte, bool, error)
}

type FlagFile struct {
	*flag.FlagSet
}

type SyntaxError struct {
	msg  string
	path string
	line int
}

func (e *SyntaxError) Error() string {
	var prefix string
	if e.path != "" {
		prefix = e.path + ":"
	}
	return fmt.Sprintf("%s%d: %s", prefix, e.line, e.msg)
}

func NewFlagFile(path string) (ff *FlagFile, err error) {
	file, err := os.Open(path)
	if err != nil {
		return
	}
	ff = &FlagFile{flag.NewFlagSet(os.Args[0], flag.ExitOnError)}
	err = ff.ReadFlags(file)
	synerr, ok := err.(*SyntaxError)
	if ok {
		synerr.path = path
	}
	return
}

func readLine(reader LineReader) (string, error) {
	var buffer bytes.Buffer
	for {
		line_bytes, isPrefix, err := reader.ReadLine()
		if err != nil {
			return "", err
		}
		buffer.Write(line_bytes)
		if !isPrefix {
			break
		}
	}
	return buffer.String(), nil
}

func readArg(reader *bufio.Reader) (name string, val string, n int, err error) {
	for {
		var line string
		n++
		line, err = readLine(reader)
		if err != nil {
			return
		}
		if idx := strings.IndexRune(line, '#'); idx >= 0 {
			line = line[:idx]
		}
		line = strings.TrimSpace(line)
		if len(line) != 0 {
			parts := strings.SplitN(line, "=", 2)
			if len(parts) != 2 {
				err = errors.New(fmt.Sprintf("assignment required: %#v", line))
				return
			}
			name = strings.TrimSpace(parts[0])
			val = strings.TrimSpace(parts[1])
			return
		}
	}
	return
}

func (ff *FlagFile) ReadFlags(reader io.Reader) error {
	lineNo := 0
	bufReader := bufio.NewReader(reader)
	for {
		name, val, n, err := readArg(bufReader)
		lineNo += n
		if err != nil {
			if err == io.EOF {
				break
			}
			return &SyntaxError{err.Error(), "", lineNo}
		}
		flag := flag.Lookup(name)
		if flag == nil {
			return &SyntaxError{
				fmt.Sprintf("undefined flag: %s", name),
				"",
				lineNo,
			}
		}
		if err = flag.Value.Set(val); err != nil {
			return &SyntaxError{err.Error(), "", lineNo}
		}
	}
	return nil
}
