package node

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"

	netx "github.com/rdisa/agent-memory-store/internal/net"
	"github.com/rdisa/agent-memory-store/internal/types"
)

// HTTPServer exposes the client API, inter-node replication, and debug/fault
// toggles. Debug endpoints exist so Docker e2e can partition without iptables.
type HTTPServer struct {
	Node   *Node
	HTTP   *http.Server
	ln     net.Listener
	Addr   string
}

func NewHTTPServer(n *Node, addr string) *HTTPServer {
	s := &HTTPServer{Node: n}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/write", s.handleWrite)
	mux.HandleFunc("/read", s.handleRead)
	mux.HandleFunc("/delete", s.handleDelete)
	mux.HandleFunc("/history", s.handleHistory)
	mux.HandleFunc("/fact", s.handleFact)
	mux.HandleFunc("/facts", s.handleFacts)
	mux.HandleFunc("/tag", s.handleTag)
	mux.HandleFunc("/internal/replicate", s.handleReplicate)
	mux.HandleFunc("/internal/read", s.handleInternalRead)
	mux.HandleFunc("/internal/digest", s.handleDigest)
	mux.HandleFunc("/internal/hint", s.handleReplicate)
	mux.HandleFunc("/debug/kv", s.handleDebugKV)
	mux.HandleFunc("/debug/hints", s.handleDebugHints)
	mux.HandleFunc("/debug/fault", s.handleFault)
	mux.HandleFunc("/debug/anti-entropy", s.handleAntiEntropy)
	mux.HandleFunc("/debug/handoff", s.handleHandoff)
	s.HTTP = &http.Server{Addr: addr, Handler: mux}
	return s
}

func (s *HTTPServer) Start() error {
	ln, err := net.Listen("tcp", s.HTTP.Addr)
	if err != nil {
		return err
	}
	s.ln = ln
	s.Addr = ln.Addr().String()
	go func() { _ = s.HTTP.Serve(ln) }()
	return nil
}

func (s *HTTPServer) Close() error {
	if s.HTTP != nil {
		return s.HTTP.Close()
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func readJSON(r *http.Request, dst any) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(dst)
}

func (s *HTTPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{"ok": true, "id": s.Node.ID, "n": s.Node.N()})
}

type writeJSONReq struct {
	Key       string `json:"key"`
	Data      string `json:"data"`
	Policy    string `json:"policy"`
	W         int    `json:"w"`
	ClientTS  int64  `json:"client_ts"`
	ORSetOp   string `json:"orset_op"`
	ORSetElem string `json:"orset_elem"`
	Tombstone bool   `json:"tombstone"`
}

func (s *HTTPServer) handleWrite(w http.ResponseWriter, r *http.Request) {
	var req writeJSONReq
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	pol, err := types.ParsePolicy(req.Policy)
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	resp := s.Node.Write(r.Context(), types.WriteReq{
		Key: req.Key, Data: []byte(req.Data), Policy: pol, W: req.W,
		ClientTS: req.ClientTS, ORSetOp: req.ORSetOp, ORSetElem: req.ORSetElem, Tombstone: req.Tombstone,
	})
	status := 200
	if !resp.OK {
		status = 503
	}
	writeJSON(w, status, resp)
}

func (s *HTTPServer) handleRead(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	q := r.URL.Query().Get("r")
	rr := 1
	if q != "" {
		_, _ = parseInt(q, &rr)
	}
	if r.Method == http.MethodPost {
		var body struct {
			Key string `json:"key"`
			R   int    `json:"r"`
		}
		if err := readJSON(r, &body); err == nil {
			if body.Key != "" {
				key = body.Key
			}
			if body.R > 0 {
				rr = body.R
			}
		}
	}
	resp := s.Node.Read(r.Context(), types.ReadReq{Key: key, R: rr})
	status := 200
	if resp.Error != "" {
		status = 503
	}
	writeJSON(w, status, resp)
}

func (s *HTTPServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	var req writeJSONReq
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	resp := s.Node.Delete(r.Context(), req.Key, req.W)
	status := 200
	if !resp.OK {
		status = 503
	}
	writeJSON(w, status, resp)
}

func (s *HTTPServer) handleHistory(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if e := r.URL.Query().Get("entity"); e != "" {
		key = e + ":" + r.URL.Query().Get("attribute")
	}
	writeJSON(w, 200, s.Node.Replica.History(key))
}

func (s *HTTPServer) handleFact(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			Entity    string `json:"entity"`
			Attribute string `json:"attribute"`
			Value     string `json:"value"`
			Policy    string `json:"policy"`
			W         int    `json:"w"`
		}
		if err := readJSON(r, &req); err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		pol, _ := types.ParsePolicy(req.Policy)
		resp := s.Node.Write(r.Context(), types.WriteReq{
			Key: req.Entity + ":" + req.Attribute, Data: []byte(req.Value), Policy: pol, W: req.W,
		})
		status := 200
		if !resp.OK {
			status = 503
		}
		writeJSON(w, status, resp)
	default:
		entity := r.URL.Query().Get("entity")
		attr := r.URL.Query().Get("attribute")
		rr := 2
		_, _ = parseInt(r.URL.Query().Get("r"), &rr)
		resp := s.Node.Read(r.Context(), types.ReadReq{Key: entity + ":" + attr, R: rr})
		writeJSON(w, 200, resp)
	}
}

func (s *HTTPServer) handleFacts(w http.ResponseWriter, r *http.Request) {
	entity := r.URL.Query().Get("entity")
	writeJSON(w, 200, s.Node.Replica.KV.Prefix(entity+":"))
}

func (s *HTTPServer) handleTag(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Entity string `json:"entity"`
		Tag    string `json:"tag"`
		W      int    `json:"w"`
	}
	if r.Method != http.MethodGet {
		if err := readJSON(r, &req); err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
	} else {
		req.Entity = r.URL.Query().Get("entity")
		req.Tag = r.URL.Query().Get("tag")
	}
	if req.W == 0 {
		req.W = 2
	}
	op := "add"
	if r.Method == http.MethodDelete {
		op = "remove"
	}
	if r.Method == http.MethodGet {
		resp := s.Node.Read(r.Context(), types.ReadReq{Key: req.Entity + ":tags", R: 2})
		writeJSON(w, 200, resp)
		return
	}
	resp := s.Node.Write(r.Context(), types.WriteReq{
		Key: req.Entity + ":tags", Policy: types.PolicyCRDTORSet, W: req.W, ORSetOp: op, ORSetElem: req.Tag,
	})
	status := 200
	if !resp.OK {
		status = 503
	}
	writeJSON(w, status, resp)
}

func (s *HTTPServer) handleReplicate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Key   string      `json:"key"`
		Value types.Value `json:"value"`
		From  string      `json:"from"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	if err := s.Node.ApplyReplicate(types.NodeID(body.From), body.Key, body.Value); err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (s *HTTPServer) handleInternalRead(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Key  string `json:"key"`
		From string `json:"from"`
	}
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	v, found, err := s.Node.ServeRead(types.NodeID(body.From), body.Key)
	if err != nil {
		writeJSON(w, 503, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"value": v, "found": found})
}

func (s *HTTPServer) handleDigest(w http.ResponseWriter, r *http.Request) {
	m, err := s.Node.ServeDigest("")
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"values": m})
}

func (s *HTTPServer) handleDebugKV(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		writeJSON(w, 200, s.Node.Replica.Snapshot())
		return
	}
	v, ok := s.Node.Replica.Get(key)
	writeJSON(w, 200, map[string]any{"found": ok, "value": v})
}

func (s *HTTPServer) handleDebugHints(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{"count": s.Node.Hints.Count(), "targets": s.Node.Hints.Targets()})
}

func (s *HTTPServer) handleFault(w http.ResponseWriter, r *http.Request) {
	ht, ok := s.Node.Net.(*netx.HTTPTransport)
	if !ok {
		writeJSON(w, 400, map[string]string{"error": "fault toggle only on HTTP transport"})
		return
	}
	var req struct {
		Action string   `json:"action"` // block | unblock | heal
		Peers  []string `json:"peers"`
	}
	if err := readJSON(r, &req); err != nil && err != io.EOF {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	var ids []types.NodeID
	for _, p := range req.Peers {
		ids = append(ids, types.NodeID(p))
	}
	switch strings.ToLower(req.Action) {
	case "block", "partition":
		ht.Block(ids...)
	case "unblock", "recover":
		ht.Unblock(ids...)
	case "heal":
		ht.Unblock()
	default:
		writeJSON(w, 400, map[string]string{"error": "action must be block|unblock|heal"})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "action": req.Action, "peers": req.Peers})
}

func (s *HTTPServer) handleAntiEntropy(w http.ResponseWriter, r *http.Request) {
	push, pull := s.Node.AntiEntropyRound(r.Context())
	writeJSON(w, 200, map[string]int{"pushed": push, "pulled": pull})
}

func (s *HTTPServer) handleHandoff(w http.ResponseWriter, r *http.Request) {
	n := s.Node.ReplayHints(r.Context())
	writeJSON(w, 200, map[string]int{"delivered": n})
}

func parseInt(s string, dst *int) (int, bool) {
	if s == "" {
		return 0, false
	}
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	*dst = n
	return n, true
}
