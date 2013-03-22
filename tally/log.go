package tally

import (
	"io"
	"log"
	"log/syslog"
)

var infologger *log.Logger
var errorlogger *log.Logger

func LogTo(out io.Writer) {
	infologger = log.New(out, "", log.LstdFlags)
	errorlogger = infologger
}

func LogToSyslog() {
	infologger, _ = syslog.NewLogger(syslog.LOG_INFO, 0)
	errorlogger, _ = syslog.NewLogger(syslog.LOG_ERR, 0)
}

func infolog(format string, params ...interface{}) {
	if infologger != nil {
		infologger.Printf(format, params...)
	}
}

func errorlog(format string, params ...interface{}) {
	if errorlogger != nil {
		errorlogger.Printf("ERROR: "+format, params...)
	}
}
