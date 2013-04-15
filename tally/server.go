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
	lastReport    time.Time
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
		server.addInternalStats(snapshot)
		server.lastReport = nextStart
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

func (server *Server) addInternalStats(snapshot *Snapshot) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	snapshot.Report("tallier.mem.alloc", float64(ms.Alloc))
	snapshot.Report("tallier.mem.total", float64(ms.TotalAlloc))
	snapshot.Report("tallier.mem.sys", float64(ms.Sys))
	snapshot.Report("tallier.mem.lookups", float64(ms.Lookups))
	snapshot.Report("tallier.mem.mallocs", float64(ms.Mallocs))
	snapshot.Report("tallier.mem.frees", float64(ms.Frees))

	snapshot.Report("tallier.mem.heap.alloc", float64(ms.HeapAlloc))
	snapshot.Report("tallier.mem.heap.sys", float64(ms.HeapSys))
	snapshot.Report("tallier.mem.heap.idle", float64(ms.HeapIdle))
	snapshot.Report("tallier.mem.heap.inuse", float64(ms.HeapInuse))
	snapshot.Report("tallier.mem.heap.released", float64(ms.HeapReleased))
	snapshot.Report("tallier.mem.heap.objects", float64(ms.HeapObjects))

	snapshot.Report("tallier.mem.stack.inuse", float64(ms.StackInuse))
	snapshot.Report("tallier.mem.stack.sys", float64(ms.StackSys))
	snapshot.Report("tallier.mem.stack.mspaninuse", float64(ms.MSpanInuse))
	snapshot.Report("tallier.mem.stack.mspansys", float64(ms.MSpanSys))
	snapshot.Report("tallier.mem.stack.mcacheinuse", float64(ms.MCacheInuse))
	snapshot.Report("tallier.mem.stack.mcachesys", float64(ms.MCacheSys))
	snapshot.Report("tallier.mem.stack.buckhashsys", float64(ms.BuckHashSys))

	snapshot.Report("tallier.mem.nextgc", float64(ms.NextGC))
	snapshot.Report("tallier.mem.lastgc", float64(ms.LastGC))
	snapshot.Report("tallier.mem.pausetotalns", float64(ms.PauseTotalNs))
	snapshot.Report("tallier.mem.numgc", float64(ms.NumGC))

	if ms.NumGC > 0 {
		lastGC := time.Unix(0, int64(ms.LastGC))
		if lastGC.After(server.lastReport) {
			snapshot.Report("tallier.mem.pause",
				float64(ms.PauseNs[(ms.NumGC+255)%256])/1000000000,
				lastGC)
		}
	}

	snapshot.Report("tallier.num_workers", float64(snapshot.numChildren))
	tot := len(snapshot.counts) + len(snapshot.timings) + len(snapshot.reports) + 1
	snapshot.Report("tallier.num_stats", float64(tot))
}
