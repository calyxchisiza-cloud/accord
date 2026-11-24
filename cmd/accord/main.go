package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"time"

	"accord"
)

func main() {
	id := flag.String("id", "a", "node id")
	raftAddr := flag.String("raft", "127.0.0.1:8201", "raft listen address")
	apiAddr := flag.String("api", "127.0.0.1:8211", "http listen address")
	peerList := flag.String("peers", "a=127.0.0.1:8201,b=127.0.0.1:8202,c=127.0.0.1:8203", "id=host:port list")
	dir := flag.String("dir", "data", "durable state directory")
	flag.Parse()

	peers, ids, err := parsePeers(*peerList)
	if err != nil {
		fail(err)
	}
	ln, err := net.Listen("tcp", *raftAddr)
	if err != nil {
		fail(err)
	}
	node, err := accord.Open(*dir, *id, ids)
	if err != nil {
		fail(err)
	}
	nt, err := accord.Attach(node, ln, peers)
	if err != nil {
		fail(err)
	}
	defer nt.Close()
	api, err := accord.Serve(*apiAddr, node)
	if err != nil {
		fail(err)
	}
	log.Printf("accord %s raft %s api %s", *id, ln.Addr(), *apiAddr)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	<-ctx.Done()

	shut, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = api.Shutdown(shut)
}

func parsePeers(raw string) (map[string]string, []string, error) {
	peers := map[string]string{}
	var ids []string
	for _, part := range strings.Split(raw, ",") {
		id, addr, ok := strings.Cut(part, "=")
		if !ok || id == "" || addr == "" {
			return nil, nil, fmt.Errorf("peer %q", part)
		}
		peers[id] = addr
		ids = append(ids, id)
	}
	return peers, ids, nil
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
