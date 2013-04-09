package tally

import (
	"errors"
	"net/http"
	"net/url"
	"testing"
	"time"
)

type TestPost struct {
	path []string
	data map[string]string
}

type TestPoster struct {
	request  chan TestPost
	response chan error
}

func (poster *TestPoster) Post(path []string,
	data map[string]string) (*http.Response, error) {
	poster.request <- TestPost{path, data}
	return nil, <-poster.response
}

func TestHeartMonitor(t *testing.T) {
	poster := TestPoster{make(chan TestPost), make(chan error)}
	harold, err := NewHarold("address", "secret", &poster)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	i := time.Duration(1) * time.Second
	intervals := harold.HeartMonitor("tag")

	intervals <- i
	expected := make(map[string]string)
	expected["tag"] = "tag"
	expected["interval"] = "1"
	req := <-poster.request
	poster.response <- nil
	if s, ok := assertDeepEqual(expected, req.data); !ok {
		t.Error(s)
	}

	// send i and hold heartbeat request. send j and k, then return an error on
	// the i heartbeat. this should result in k being posted next.
	j := time.Duration(2) * time.Second
	k := time.Duration(3) * time.Second

	intervals <- i
	req = <-poster.request
	intervals <- j
	intervals <- k
	if s, ok := assertDeepEqual(expected, req.data); !ok {
		t.Error(s)
	}

	expected["interval"] = "3"
	poster.response <- errors.New("fake error")
	req = <-poster.request
	if s, ok := assertDeepEqual(expected, req.data); !ok {
		t.Error(s)
	}
}

func TestMakeUrl(t *testing.T) {
	harold, err := NewHarold("address", "secret")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	expected := "http://address/harold/x/secret"
	result := harold.makeUrl("x")
	if expected != result {
		t.Errorf("expected %v, result was %v", expected, result)
	}

	expected = "http://address/harold/x/y/z/secret"
	result = harold.makeUrl("x", "y", "z")
	if expected != result {
		t.Errorf("expected %v, result was %v", expected, result)
	}
}

func TestMakeParams(t *testing.T) {
	var data = make(map[string]string)
	data["x"] = "1"
	data["y"] = "2"
	data["z"] = "3"

	expected, err := url.ParseQuery("x=1&y=2&z=3")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	result := makeParams(data)
	if s, ok := assertDeepEqual(expected, result); !ok {
		t.Error(s)
	}
}
