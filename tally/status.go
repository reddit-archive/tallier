package tally

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
)

type StatusHandler func(*Server, http.ResponseWriter, *http.Request)

func ServeStatus(server *Server) {
	http.Handle("/", makePage(server, StatusPage))
	go http.ListenAndServe(fmt.Sprintf(":%d", server.receiverPort), nil)
}

func makePage(s *Server, f StatusHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-type", "text/plain; charset=utf-8")
		f(s, w, r)
	}
}

func StatusPage(s *Server, w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "ok!")
}
