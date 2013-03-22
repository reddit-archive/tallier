package tally

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
)

type GraphiteDialer interface {
	Dial(*net.TCPAddr) (io.WriteCloser, error)
}

// Graphite is a client for sending stat reports to a graphite (carbon) server.
type Graphite struct {
	addr   *net.TCPAddr
	dialer GraphiteDialer
}

func (graphite *Graphite) Dial(addr *net.TCPAddr) (io.WriteCloser, error) {
	return net.DialTCP("tcp", nil, addr)
}

func NewGraphite(address string,
	options ...interface{}) (client *Graphite, err error) {
	addr, err := net.ResolveTCPAddr("tcp", address)
	client = &Graphite{addr, nil}
	client.dialer = client
	for _, option := range options {
		switch option.(type) {
		case GraphiteDialer:
			client.dialer = option.(GraphiteDialer)
		default:
			err = errors.New(fmt.Sprintf("invalid graphite option %T", option))
			return
		}
	}
	return
}

// SendReport takes a snapshot and submits all its stats to graphite.
func (graphite *Graphite) SendReport(snapshot *Snapshot) (err error) {
	conn, err := graphite.dialer.Dial(graphite.addr)
	if err != nil {
		return
	}
	defer conn.Close()
	msg := strings.Join(snapshot.GraphiteReport(), "")
	_, err = conn.Write([]byte(msg))
	return
}
