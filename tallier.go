package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"runtime"
	"time"

	"github.com/reddit/tallier/tally"
)

var configFlag = flag.String("config", "",
	"read flags from this file; overrides any command line settings")

var portFlag = flag.Int("port", 8081, "udp port to listen for statgrams on")

var numWorkersFlag = flag.Int("numWorkers",
	int(math.Min(1, float64(runtime.NumCPU()-1))),
	"number of parallel workers for parsing and accumulating stats")

var flushIntervalFlag = flag.Duration("flushInterval",
	time.Duration(4)*time.Second,
	"interval at which stats are flushed to graphite")

var graphiteFlag = flag.String("graphite", "",
	"address of graphite (carbon) server")

var haroldFlag = flag.String("harold", "",
	"address of harold service (REQUIRES -haroldSecret)")

var haroldSecretFlag = flag.String("haroldSecret", "",
	"secret for authenticating with harold service")

var logtoFlag = flag.String("logto", "stdout",
	"destination for logging (one of: stdout, stderr, syslog)")

func main() {
	flag.Parse()
	if *configFlag != "" {
		_, err := tally.NewFlagFile(*configFlag)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %s\n", err)
			os.Exit(2)
		}
	}

	switch *logtoFlag {
	case "stdout":
		tally.LogTo(os.Stdout)
	case "stderr":
		tally.LogTo(os.Stderr)
	case "syslog":
		tally.LogToSyslog()
	default:
		fmt.Fprintf(os.Stderr,
			"error: -logto must be one of stdout, stderr, or syslog\n")
		os.Exit(2)
	}

	if *graphiteFlag == "" {
		fmt.Fprintf(os.Stderr, "-graphite is required\n")
		os.Exit(2)
	}
	graphite, err := tally.NewGraphite(*graphiteFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", err)
		os.Exit(1)
	}

	var harold *tally.Harold
	if *haroldFlag != "" {
		if *haroldSecretFlag == "" {
			fmt.Fprintf(os.Stderr, "harold requires -haroldSecret to be set\n")
			os.Exit(2)
		}
		harold, err = tally.NewHarold(*haroldFlag, *haroldSecretFlag)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %s\n", err)
			os.Exit(1)
		}
	}

	server := tally.NewServer(
		*portFlag, *numWorkersFlag, *flushIntervalFlag, graphite, harold)

	err = server.Loop()
	if err != nil {
		fmt.Fprintf(os.Stderr, "loop terminated with error: %s\n", err)
		os.Exit(1)
	}
}
