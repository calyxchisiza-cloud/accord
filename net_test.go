package accord

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHTTPFollowerRejectsWrite(t *testing.T) {
	c := testCluster(t, "a")
	body := httptest.NewRequest(http.MethodPut, "/v1/kv/color", strings.NewReader("blue"))
	rec := httptest.NewRecorder()
	Handler(c.nodes["a"]).ServeHTTP(rec, body)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status %d %s", rec.Code, rec.Body.String())
	}
}

func TestHTTPPutGetAndLock(t *testing.T) {
	c := testCluster(t, "a")
	if err := c.Campaign("a"); err != nil {
		t.Fatal(err)
	}
	h := Handler(c.nodes["a"])

	put := httptest.NewRequest(http.MethodPut, "/v1/kv/color", strings.NewReader("blue"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, put)
	if rec.Code != http.StatusOK {
		t.Fatalf("put %d %s", rec.Code, rec.Body.String())
	}

	get := httptest.NewRequest(http.MethodGet, "/v1/kv/color", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, get)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "blue") {
		t.Fatalf("get %d %s", rec.Code, rec.Body.String())
	}

	lock := httptest.NewRequest(http.MethodPost, "/v1/locks/payments", strings.NewReader(`{"owner":"worker-1"}`))
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, lock)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "token") {
		t.Fatalf("lock %d %s", rec.Code, rec.Body.String())
	}

	again := httptest.NewRequest(http.MethodPost, "/v1/locks/payments", strings.NewReader(`{"owner":"worker-2"}`))
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, again)
	if rec.Code != http.StatusConflict {
		t.Fatalf("held %d %s", rec.Code, rec.Body.String())
	}

	missing := httptest.NewRequest(http.MethodGet, "/v1/kv/other", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, missing)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("missing %d", rec.Code)
	}
}

func TestLiveClusterAgrees(t *testing.T) {
	dir := t.TempDir()
	ids := []string{"a", "b", "c"}
	lns := map[string]net.Listener{}
	peers := map[string]string{}
	for _, id := range ids {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns[id] = ln
		peers[id] = ln.Addr().String()
	}
	var nodes []*Node
	var nets []*Net
	for _, id := range ids {
		node, err := Open(filepath.Join(dir, id), id, ids)
		if err != nil {
			t.Fatal(err)
		}
		nt, err := Attach(node, lns[id], peers)
		if err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, node)
		nets = append(nets, nt)
	}
	t.Cleanup(func() {
		for _, nt := range nets {
			nt.Close()
		}
	})

	deadline := time.Now().Add(10 * time.Second)
	var leader *Node
	for time.Now().Before(deadline) {
		var found []*Node
		for _, node := range nodes {
			if node.Status().Role == roleLeader {
				found = append(found, node)
			}
		}
		if len(found) == 1 {
			leader = found[0]
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if leader == nil {
		t.Fatal("no leader")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := leader.Submit(ctx, Entry{Kind: "put", Key: "color", Value: "blue"})
	if err != nil {
		t.Fatal(err)
	}
	if out.value != "blue" {
		t.Fatalf("submit %q", out.value)
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ready := true
		for _, node := range nodes {
			got, ok := node.local("color")
			if !ok || got != "blue" {
				ready = false
				break
			}
		}
		if ready {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("value did not reach every node")
}
