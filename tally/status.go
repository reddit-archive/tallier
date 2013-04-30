package tally

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	_ "net/http/pprof"
	"sort"
	"strings"
	"time"
)

type StatusRequest struct {
	s    *Server
	w    http.ResponseWriter
	r    *http.Request
	t    *template.Template
	data interface{}
}

type statusHandler interface {
	handle(*StatusRequest)
}

type statusHandlerWithTemplate interface {
	statusHandler
	getTemplate() string
}

func ServeStatus(server *Server) error {
	pages := map[string]statusHandler{
		"/":         statusPage{},
		"/strings/": stringsPage{},
	}

	var err error
	for path, h := range pages {
		e := handleStatus(server, path, h)
		if e != nil {
			err = e
			errorlog("error: %s", err)
		}
	}
	if err == nil {
		addr := fmt.Sprintf("%s:%d", server.receiverHost, server.receiverPort)
		go http.ListenAndServe(addr, nil)
	}
	return err
}

func handleStatus(server *Server, path string, h statusHandler) (err error) {
	err = makePage(server, "/json"+path, h, false)
	if err == nil {
		err = makePage(server, path, h, true)
	}
	return
}

func makePage(s *Server, path string, h statusHandler,
	applyTemplate bool) error {
	var t *template.Template
	switch h.(type) {
	case statusHandlerWithTemplate:
		text := h.(statusHandlerWithTemplate).getTemplate()
		var e error
		t, e = template.New(path).Parse(text)
		if e != nil {
			return e
		}
	}
	var f http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		req := StatusRequest{s, w, r, t, nil}
		h.handle(&req)
		if req.data != nil {
			if applyTemplate && req.t != nil {
				err := req.t.Execute(req.w, req.data)
				if err != nil {
					errorlog("template exec error: %s", err)
				}
			} else {
				w.Header().Set("Content-type",
					"application/json; charset=utf-8")
				json.NewEncoder(req.w).Encode(req.data)
			}
		}
	}
	http.Handle(path, f)
	return nil
}

type statusPage struct{}

func (statusPage) getTemplate() string {
	return `
        <h1>tallier status</h1>
        <h4><a href="/strings/tallier.samples">top stats</a></h4>
        <h4><a href="/strings/">strings</a><h4>
        <h4><a href="/debug/pprof">cpu profile</a></h4>
`
}

func (statusPage) handle(req *StatusRequest) {
	// this function only exists to register the handler
	req.data = map[string]interface{}{}
}

type stringsPage struct{}

func (stringsPage) getTemplate() string {
	return `
{{if .str}}
  <table>
    <thead>
      <tr>
        <th>rank</th><th>string</th><th>minute</th><th>hour</th>
      </tr>
    </thead>
    <tbody>
      {{range .items}}
        <tr>
          <td>{{.rank}}</td>
          <td>{{.key}}</td>
          <td>{{printf "%.2g" .minute.rate}} ({{.minute.total}})</td>
          <td>{{printf "%.2g" .hour.rate}} ({{.hour.total}})</td>
        </tr>
      {{end}}
    </tbody>
  </table>
{{else}}
  {{range $_, $key := .keys}}
    <a href="/strings/{{$key}}">{{$key}}</a><br>
  {{end}}
{{end}}`
}

func (stringsPage) handle(req *StatusRequest) {
	var str string
	var needSort bool
	if strings.HasPrefix(req.r.URL.Path, "/json/strings/") {
		str = req.r.URL.Path[len("/json/strings/"):]
	} else if strings.HasPrefix(req.r.URL.Path, "/strings/") {
		str = req.r.URL.Path[len("/strings/"):]
		needSort = true
	}
	if str != "" {
		stringPage(req, str)
		return
	}

	if req.s.snapshot == nil || len(req.s.snapshot.stringCounts) == 0 {
		fmt.Fprintf(req.w, "no stats to report yet")
		return
	}
	data := make([]string, 0, len(req.s.snapshot.stringCounts))
	for key, _ := range req.s.snapshot.stringCounts {
		data = append(data, key)
	}
	if needSort {
		sort.Strings(data)
	}
	req.data = map[string]interface{}{"keys": data}
}

func stringPage(req *StatusRequest, key string) {
	if req.s.snapshot == nil {
		http.NotFound(req.w, req.r)
		return
	}
	fcs, ok := req.s.snapshot.stringCounts[key]
	if !ok {
		http.NotFound(req.w, req.r)
		return
	}
	items := fcs.SortedItems()
	levels := []string{"minute", "hour"}
	data := make([]map[string]interface{}, len(items))
	for i, item := range items {
		data[i] = map[string]interface{}{
			"key":  item.key,
			"rank": i + 1,
		}
		for j, level := range levels {
			c := (*(item.count))[j+1]
			data[i][level] = map[string]interface{}{
				"rate":  c.RatePer(time.Second),
				"total": c.Current,
			}
		}
	}
	req.data = map[string]interface{}{
		"str":   key,
		"items": data,
	}
}
