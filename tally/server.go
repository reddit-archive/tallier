package tally

import (
	"errors"
	"fmt"
	"net"
	"runtime"
	"time"
)

type Server struct {
	receiverHost  string
	receiverPort  int
	numWorkers    int
	flushInterval time.Duration
	graphite      *Graphite
	harold        *Harold
	conn          *net.UDPConn
	snapshot      *Snapshot
}

func NewServer(host string, port int, numWorkers int,
	flushInterval time.Duration, graphite *Graphite, harold *Harold) *Server {
	return &Server{
		receiverHost:  host,
		receiverPort:  port,
		numWorkers:    numWorkers,
		flushInterval: flushInterval,
		graphite:      graphite,
		harold:        harold,
	}
}

func (server *Server) setup() error {
	runtime.GOMAXPROCS(server.numWorkers + 1)
	receiver_addr, err := net.ResolveUDPAddr("udp",
		fmt.Sprintf("%s:%d", server.receiverHost, server.receiverPort))
	if err != nil {
		return err
	}
	server.conn, err = net.ListenUDP("udp", receiver_addr)
	return err
}

func (server *Server) Loop() error {
	var intervals chan time.Duration
	if err := server.setup(); err != nil {
		return err
	}
	if server.harold != nil {
		intervals = server.harold.HeartMonitor("tallier")
	}
	snapchan := Aggregate(server.conn, server.numWorkers)
	ServeStatus(server)
	infolog("running")
	server.snapshot = NewSnapshot()
	server.snapshot.stringCountIntervals = []time.Duration{
		time.Minute, time.Hour, time.Duration(24) * time.Hour}
	server.snapshot.start = time.Now()
	tick := time.Tick(server.flushInterval)
	for {
		<-tick
		snapchan <- server.snapshot
		snapshot := <-snapchan
		nextStart := time.Now()
		for {
			infolog("sending snapshot with %d stats to graphite",
				snapshot.NumStats())
			var err error
			if err = server.graphite.SendReport(snapshot); err == nil {
				break
			}
			errorlog("failed to send graphite report: %s", err)
			time.Sleep(time.Second)
		}
		if server.harold != nil {
			intervals <- 3 * server.flushInterval
		}
		snapshot.Flush()
		snapshot.start = nextStart
	}
	return errors.New("server loop terminated")
}
