package accord

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"time"
)

// Handler serves the key and lock API for one node.
// A follower answers status, and refuses a read or a write with the leader id.
func Handler(n *Node) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, n.Status())
	})
	mux.HandleFunc("PUT /v1/kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, maxValue+1))
		if err != nil {
			writeErr(w, http.StatusBadRequest, ErrInvalid, n.Status().Leader)
			return
		}
		if len(body) > maxValue {
			writeErr(w, http.StatusRequestEntityTooLarge, ErrTooLarge, n.Status().Leader)
			return
		}
		out, err := submit(r, n, Entry{Kind: "put", Key: r.PathValue("key"), Value: string(body)})
		if err != nil {
			writeAPI(w, err, n.Status().Leader)
			return
		}
		writeJSON(w, http.StatusOK, map[string]uint64{"index": out.index})
	})
	mux.HandleFunc("GET /v1/kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		out, err := submit(r, n, Entry{Kind: "read", Key: r.PathValue("key")})
		if err != nil {
			writeAPI(w, err, n.Status().Leader)
			return
		}
		if !out.found {
			writeErr(w, http.StatusNotFound, ErrNotFound, n.Status().Leader)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"value": out.value})
	})
	mux.HandleFunc("DELETE /v1/kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		out, err := submit(r, n, Entry{Kind: "del", Key: r.PathValue("key")})
		if err != nil {
			writeAPI(w, err, n.Status().Leader)
			return
		}
		writeJSON(w, http.StatusOK, map[string]uint64{"index": out.index})
	})
	mux.HandleFunc("POST /v1/locks/{key}", func(w http.ResponseWriter, r *http.Request) {
		owner, err := readOwner(r)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err, n.Status().Leader)
			return
		}
		out, err := submit(r, n, Entry{Kind: "lock", Key: r.PathValue("key"), Owner: owner})
		if err != nil {
			writeAPI(w, err, n.Status().Leader)
			return
		}
		writeJSON(w, http.StatusOK, map[string]uint64{"token": out.token})
	})
	mux.HandleFunc("DELETE /v1/locks/{key}", func(w http.ResponseWriter, r *http.Request) {
		out, err := submit(r, n, Entry{Kind: "unlock", Key: r.PathValue("key"), Owner: r.URL.Query().Get("owner")})
		if err != nil {
			writeAPI(w, err, n.Status().Leader)
			return
		}
		writeJSON(w, http.StatusOK, map[string]uint64{"index": out.index})
	})
	return mux
}

func submit(r *http.Request, n *Node, e Entry) (outcome, error) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	return n.Submit(ctx, e)
}

func readOwner(r *http.Request) (string, error) {
	var body struct {
		Owner string `json:"owner"`
	}
	dec := json.NewDecoder(io.LimitReader(r.Body, 1024))
	if err := dec.Decode(&body); err != nil {
		return "", ErrInvalid
	}
	if err := validOwner(body.Owner); err != nil {
		return "", err
	}
	return body.Owner, nil
}

func writeAPI(w http.ResponseWriter, err error, leader string) {
	switch err {
	case ErrNotLeader, ErrHeld, ErrNotHeld:
		writeErr(w, http.StatusConflict, err, leader)
	case ErrNotFound:
		writeErr(w, http.StatusNotFound, err, leader)
	case ErrInvalid:
		writeErr(w, http.StatusBadRequest, err, leader)
	case ErrTooLarge:
		writeErr(w, http.StatusRequestEntityTooLarge, err, leader)
	case ErrTimeout, ErrNoQuorum:
		writeErr(w, http.StatusServiceUnavailable, err, leader)
	case ErrFull:
		writeErr(w, http.StatusInsufficientStorage, err, leader)
	case ErrClosed:
		writeErr(w, http.StatusServiceUnavailable, err, leader)
	default:
		writeErr(w, http.StatusInternalServerError, err, leader)
	}
}

func writeErr(w http.ResponseWriter, code int, err error, leader string) {
	writeJSON(w, code, map[string]string{"error": err.Error(), "leader": leader})
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

// Serve listens on a loopback address.
func Serve(addr string, n *Node) (*http.Server, error) {
	if err := loopback(addr); err != nil {
		return nil, err
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:           Handler(n),
		ReadHeaderTimeout: 5 * time.Second,
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	go func() { _ = srv.Serve(ln) }()
	return srv, nil
}

func loopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return ErrInvalid
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return ErrInvalid
	}
	return nil
}
