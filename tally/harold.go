package tally

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type HaroldPoster interface {
	Post(path []string, data map[string]string) (*http.Response, error)
}

// Harold is a monitoring service used by reddit. We post heartbeat messages to
// harold to let it know we're alive.
// See more at https://github.com/spladug/wessex.
type Harold struct {
	address string
	secret  string
	poster  HaroldPoster
}

func makeParams(data map[string]string) (values url.Values) {
	values = make(url.Values)
	for name, value := range data {
		values.Set(name, value)
	}
	return
}

func NewHarold(address string, secret string,
	options ...interface{}) (harold *Harold, err error) {
	harold = &Harold{address: address, secret: secret}
	harold.poster = harold
	for _, option := range options {
		switch option.(type) {
		case HaroldPoster:
			harold.poster = option.(HaroldPoster)
		default:
			err = errors.New(fmt.Sprintf("invalid graphite option %T", option))
			return
		}
	}
	return
}

func (harold *Harold) makeUrl(path ...string) string {
	return fmt.Sprintf("http://%s/harold/%s/%s", harold.address,
		strings.Join(path, "/"), harold.secret)
}

func (harold *Harold) Post(path []string,
	data map[string]string) (*http.Response, error) {
	return http.PostForm(harold.makeUrl(path...), makeParams(data))
}

// Heartbeat sends a heartbeat message to harold, blocking until acknowledged.
func (harold *Harold) Heartbeat(tag string,
	interval time.Duration) (*http.Response, error) {
	data := map[string]string{
		"tag":      tag,
		"interval": fmt.Sprintf("%f", interval.Seconds()),
	}
	return harold.poster.Post([]string{"heartbeat"}, data)
}

// HeartMonitor returns a channel for the caller to send harold heartbeats to.
// It spins off a goroutine so the heartbeat channel never blocks, even if the
// harold service is not responding.
func (harold *Harold) HeartMonitor(tag string) (intervals chan time.Duration) {
	intervals = make(chan time.Duration)
	go func() {
		var alive *time.Duration // most recent interval pending to be sent
		waiting := false         // whether we're waiting on a previous RPC

		// channel for notifying end of asynchronous heartbeat RPC
		err := make(chan error)

		for {
			select {
			case interval := <-intervals:
				alive = &interval
			case e := <-err:
				if e != nil {
					errorlog("harold heartbeat failed: %#v", e)
				}
				waiting = false
			}
			if alive != nil && !waiting {
				go func(i time.Duration) {
					infolog("sending heartbeat to harold")
					_, x := harold.Heartbeat(tag, i)
					err <- x
				}(*alive)
				waiting = true
				alive = nil
			}
		}
	}()
	return
}
