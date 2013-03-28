package tally

import (
	"fmt"
	"html"
	"net/http"
	_ "net/http/pprof"
)

type StatusHandler func(*Server, http.ResponseWriter, *http.Request)

func ServeStatus(server *Server) {
	http.Handle("/", makePage(server, StatusPage))
	http.Handle("/strings/", makePage(server, StringsPage))
	go http.ListenAndServe(fmt.Sprintf(":%d", server.receiverPort), nil)
}

func makePage(s *Server, f StatusHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f(s, w, r)
	}
}

func StatusPage(s *Server, w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "<a href='\"/strings/\">strings</a><br>")
}

func StringsPage(s *Server, w http.ResponseWriter, r *http.Request) {
	if len(r.URL.Path) > len("/strings/") {
		StringPage(s, w, r, r.URL.Path[len("/strings/"):])
		return
	}

	if s.snapshot == nil || len(s.snapshot.stringCounts) == 0 {
		fmt.Fprintf(w, "no stats to report yet")
		return
	}
	for key, _ := range s.snapshot.stringCounts {
		fmt.Fprintf(w, "<a href=\"/strings/%s\">%s</a><br>",
			html.EscapeString(key), html.EscapeString(key))
	}
}

func StringPage(s *Server, w http.ResponseWriter, r *http.Request, key string) {
	if s.snapshot == nil {
		http.NotFound(w, r)
		return
	}
	fcs, ok := s.snapshot.stringCounts[key]
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-type", "text/plain; charset=utf-8")
	for i, fc := range fcs.SortedItems() {
		fmt.Fprintf(w, "%4d. %-40s %f\n", i+1, fc.key, fc.count)
	}
}
