package accord

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"sync"
	"time"
)

// Net carries Raft messages over TCP between nodes on this machine.
type Net struct {
	node   *Node
	peers  map[string]string
	ln     net.Listener
	mu     sync.Mutex
	conns  map[string]*peerConn
	done   chan struct{}
	closed bool
}

type peerConn struct {
	mu   sync.Mutex
	conn net.Conn
}

// Attach serves Raft on ln. peers maps a node id to its raft address.
// The map includes this node. Call Attach before submitting commands.
func Attach(n *Node, ln net.Listener, peers map[string]string) (*Net, error) {
	if err := loopback(ln.Addr().String()); err != nil {
		return nil, err
	}
	for _, addr := range peers {
		if err := loopback(addr); err != nil {
			return nil, err
		}
	}
	nt := &Net{
		node:  n,
		peers: peers,
		ln:    ln,
		conns: map[string]*peerConn{},
		done:  make(chan struct{}),
	}
	n.mu.Lock()
	n.send = nt.Send
	n.mu.Unlock()
	go nt.accept()
	go nt.ticks()
	return nt, nil
}

// Close stops the listener and the election timer.
func (nt *Net) Close() {
	nt.mu.Lock()
	if nt.closed {
		nt.mu.Unlock()
		return
	}
	nt.closed = true
	close(nt.done)
	ln := nt.ln
	conns := nt.conns
	nt.mu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
	for _, pc := range conns {
		pc.mu.Lock()
		if pc.conn != nil {
			_ = pc.conn.Close()
		}
		pc.mu.Unlock()
	}
	nt.node.Stop()
}

// Send writes each message to the named peer.
func (nt *Net) Send(msgs []Message) {
	for _, msg := range msgs {
		if msg.To == "" || msg.To == nt.node.id {
			continue
		}
		nt.writeOne(msg)
	}
}

func (nt *Net) ticks() {
	ticker := time.NewTicker(30 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-nt.done:
			return
		case <-ticker.C:
			nt.Send(nt.node.Tick())
		}
	}
}

func (nt *Net) accept() {
	for {
		conn, err := nt.ln.Accept()
		if err != nil {
			return
		}
		go nt.readConn(conn)
	}
}

func (nt *Net) readConn(conn net.Conn) {
	defer conn.Close()
	for {
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		msg, err := readFrame(conn)
		if err != nil {
			return
		}
		nt.Send(nt.node.Handle(msg))
	}
}

func (nt *Net) writeOne(msg Message) {
	raw, err := json.Marshal(msg)
	if err != nil || len(raw) > 1<<20 {
		return
	}
	addr := nt.peers[msg.To]
	if addr == "" {
		return
	}
	nt.mu.Lock()
	if nt.closed {
		nt.mu.Unlock()
		return
	}
	pc := nt.conns[msg.To]
	if pc == nil {
		pc = &peerConn{}
		nt.conns[msg.To] = pc
	}
	nt.mu.Unlock()

	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(raw)))
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.conn == nil {
		dialer := net.Dialer{Timeout: 200 * time.Millisecond}
		conn, err := dialer.Dial("tcp", addr)
		if err != nil {
			return
		}
		pc.conn = conn
	}
	_ = pc.conn.SetWriteDeadline(time.Now().Add(time.Second))
	if _, err = pc.conn.Write(hdr[:]); err != nil {
		_ = pc.conn.Close()
		pc.conn = nil
		return
	}
	if _, err = pc.conn.Write(raw); err != nil {
		_ = pc.conn.Close()
		pc.conn = nil
	}
}

func readFrame(r io.Reader) (Message, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Message{}, err
	}
	size := binary.BigEndian.Uint32(hdr[:])
	if size > 1<<20 {
		return Message{}, ErrTooLarge
	}
	raw := make([]byte, size)
	if _, err := io.ReadFull(r, raw); err != nil {
		return Message{}, err
	}
	var msg Message
	if err := json.Unmarshal(raw, &msg); err != nil {
		return Message{}, err
	}
	return msg, nil
}
