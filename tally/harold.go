package tally

import (
	"crypto/hmac"
	"crypto/sha1"
	// apparently we need to import this here to make go able to verify
	// certs properly. see: https://code.google.com/p/go/issues/detail?id=5058
	_ "crypto/sha512"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

type HaroldPoster interface {
	Post(path []string, data map[string]string) (*http.Response, error)
}

// Harold is a monitoring service used by reddit. We post heartbeat messages to
// harold to let it know we're alive.
// See more at https://github.com/spladug/harold.
type Harold struct {
	baseUrl *url.URL
	secret  string
	poster  HaroldPoster
	client  *http.Client
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
	baseUrl, err := url.Parse(address)
	if err != nil {
		panic("unable to parse harold url")
	}

	harold = &Harold{baseUrl: baseUrl, secret: secret}
	harold.poster = harold
	harold.client = &http.Client{}

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

func (harold *Harold) makeUrl(pathParts ...string) string {
	url := *harold.baseUrl
	url.Path = path.Join(url.Path, "harold", path.Join(pathParts...))
	return url.String()
}

func (harold *Harold) Post(path []string,
	data map[string]string) (*http.Response, error) {
	params := makeParams(data).Encode()

	request, err := http.NewRequest("POST", harold.makeUrl(path...), strings.NewReader(params))
	if err != nil {
		return nil, err
	}

	request.Header.Add("User-Agent", "tallier")
	request.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	mac := hmac.New(sha1.New, []byte(harold.secret))
	mac.Write([]byte(params))
	request.Header.Add("X-Hub-Signature", fmt.Sprintf("sha1=%x", mac.Sum(nil)))

	return harold.client.Do(request)
}

// Heartbeat sends a heartbeat message to harold, blocking until acknowledged.
func (harold *Harold) Heartbeat(tag string,
	interval time.Duration) (*http.Response, error) {
	data := map[string]string{
		"tag":      tag,
		"interval": fmt.Sprintf("%d", int(interval.Seconds())),
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
					r, x := harold.Heartbeat(tag, i)
					if x == nil && r != nil {
						r.Body.Close()
					}
					err <- x
				}(*alive)
				waiting = true
				alive = nil
			}
		}
	}()
	return
}
