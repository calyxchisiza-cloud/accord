package accord

import (
	"context"
	"hash/fnv"
	"math/rand"
	"sync"
)

const (
	roleFollower  = "follower"
	roleCandidate = "candidate"
	roleLeader    = "leader"
)

// Status is a node's current term and role.
type Status struct {
	ID     string `json:"id"`
	Role   string `json:"role"`
	Term   uint64 `json:"term"`
	Leader string `json:"leader"`
	Commit uint64 `json:"commit"`
}

// Message is one Raft RPC or its reply.
type Message struct {
	Type      string  `json:"type"`
	From      string  `json:"from"`
	To        string  `json:"to"`
	Term      uint64  `json:"term"`
	Candidate string  `json:"candidate,omitempty"`
	LastIndex uint64  `json:"lastIndex,omitempty"`
	LastTerm  uint64  `json:"lastTerm,omitempty"`
	Granted   bool    `json:"granted,omitempty"`
	Leader    string  `json:"leader,omitempty"`
	PrevIndex uint64  `json:"prevIndex,omitempty"`
	PrevTerm  uint64  `json:"prevTerm,omitempty"`
	Entries   []Entry `json:"entries,omitempty"`
	Commit    uint64  `json:"commit,omitempty"`
	Success   bool    `json:"success,omitempty"`
	Hint      uint64  `json:"hint,omitempty"`
}

// Node is one member of a cluster. Open loads its durable term, vote, and log.
type Node struct {
	id    string
	peers []string
	dir   string

	mu      sync.Mutex
	term    uint64
	vote    string
	log     []Entry
	role    string
	leader  string
	commit  uint64
	applied uint64
	next    map[string]uint64
	match   map[string]uint64
	votes   map[string]bool
	sm      *machine
	results map[uint64]outcome
	waiters map[uint64]chan struct{}
	stopped bool
	force   bool
	electN  int
	electAt int
	rng     *rand.Rand
	send    func([]Message)
}

// Open loads a node from dir. peers includes this node's own id.
func Open(dir, id string, peers []string) (*Node, error) {
	if err := checkIDs(id, peers); err != nil {
		return nil, err
	}
	st, err := loadState(dir)
	if err != nil {
		return nil, err
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	n := &Node{
		id:      id,
		peers:   append([]string{}, peers...),
		dir:     dir,
		term:    st.Term,
		vote:    st.Vote,
		log:     st.Log,
		role:    roleFollower,
		commit:  st.Commit,
		sm:      newMachine(),
		results: map[uint64]outcome{},
		waiters: map[uint64]chan struct{}{},
		next:    map[string]uint64{},
		match:   map[string]uint64{},
		rng:     rand.New(rand.NewSource(int64(h.Sum64()))),
	}
	n.electAt = 8 + n.rng.Intn(8)
	if n.commit > n.last() {
		n.commit = n.last()
	}
	n.apply()
	return n, nil
}

func checkIDs(id string, peers []string) error {
	if err := validName(id); err != nil {
		return err
	}
	seen := map[string]bool{}
	found := false
	for _, peer := range peers {
		if err := validName(peer); err != nil {
			return err
		}
		if seen[peer] {
			return ErrInvalid
		}
		seen[peer] = true
		if peer == id {
			found = true
		}
	}
	if !found {
		return ErrInvalid
	}
	return nil
}

// Status returns the role clients use to find the leader.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{ID: n.id, Role: n.role, Term: n.term, Leader: n.leader, Commit: n.commit}
}

// Submit appends a command and waits until this node applies it.
func (n *Node) Submit(ctx context.Context, e Entry) (outcome, error) {
	if err := validate(e); err != nil {
		return outcome{}, err
	}
	idx, msgs, err := n.Propose(e)
	if err != nil {
		return outcome{}, err
	}
	n.mu.Lock()
	send := n.send
	n.mu.Unlock()
	if send != nil && len(msgs) > 0 {
		send(msgs)
	}
	n.mu.Lock()
	ch := n.waiters[idx]
	_, done := n.results[idx]
	n.mu.Unlock()
	if !done {
		if ch == nil {
			return outcome{}, ErrNotLeader
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return outcome{}, ErrTimeout
		}
	}
	n.mu.Lock()
	out := n.results[idx]
	n.mu.Unlock()
	if out.err != nil {
		return out, out.err
	}
	return out, nil
}

// Propose appends a command when this node is the leader.
// The caller delivers the returned messages.
func (n *Node) Propose(e Entry) (uint64, []Message, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return 0, nil, ErrClosed
	}
	if n.role != roleLeader {
		return 0, nil, ErrNotLeader
	}
	if n.last() >= maxLog {
		return 0, nil, ErrFull
	}
	e.Term = n.term
	n.log = append(n.log, e)
	if err := n.persist(); err != nil {
		n.log = n.log[:len(n.log)-1]
		return 0, nil, err
	}
	idx := n.last()
	n.match[n.id] = idx
	n.waiters[idx] = make(chan struct{})
	n.advanceCommit()
	return idx, n.replicate(), nil
}

// Force makes the next tick start an election.
func (n *Node) Force() {
	n.mu.Lock()
	n.force = true
	n.mu.Unlock()
}

// Tick advances the election timer, or sends a heartbeat when leading.
func (n *Node) Tick() []Message {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil
	}
	if n.force {
		n.force = false
		return n.startElection()
	}
	if n.role == roleLeader {
		return n.replicate()
	}
	n.electN++
	if n.electN >= n.electAt {
		n.electN = 0
		return n.startElection()
	}
	return nil
}

// Handle applies one RPC and returns the messages to send back.
func (n *Node) Handle(m Message) []Message {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil
	}
	switch m.Type {
	case "vote":
		return []Message{n.onVote(m)}
	case "voteResp":
		return n.onVoteResp(m)
	case "append":
		return []Message{n.onAppend(m)}
	case "appendResp":
		return n.onAppendResp(m)
	default:
		return nil
	}
}

// Stop rejects new work and releases clients waiting on this node.
func (n *Node) Stop() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.stopped = true
	n.failWaiters()
}

func (n *Node) startElection() []Message {
	oldTerm, oldVote, oldRole := n.term, n.vote, n.role
	n.role = roleCandidate
	n.term++
	n.vote = n.id
	n.leader = ""
	n.votes = map[string]bool{n.id: true}
	n.electN = 0
	n.electAt = 8 + n.rng.Intn(8)
	if err := n.persist(); err != nil {
		n.term, n.vote, n.role = oldTerm, oldVote, oldRole
		return nil
	}
	if oldRole == roleLeader {
		n.failWaiters()
	}
	if n.won() {
		return n.becomeLeader()
	}
	last := n.last()
	msgs := make([]Message, 0, len(n.peers))
	for _, id := range n.peers {
		if id == n.id {
			continue
		}
		msgs = append(msgs, Message{
			Type:      "vote",
			From:      n.id,
			To:        id,
			Term:      n.term,
			Candidate: n.id,
			LastIndex: last,
			LastTerm:  n.log[last].Term,
		})
	}
	return msgs
}

func (n *Node) becomeLeader() []Message {
	n.role = roleLeader
	n.leader = n.id
	n.next = map[string]uint64{}
	n.match = map[string]uint64{}
	n.log = append(n.log, Entry{Term: n.term, Kind: "nop"})
	if err := n.persist(); err != nil {
		n.log = n.log[:len(n.log)-1]
		n.role = roleCandidate
		return nil
	}
	last := n.last()
	for _, id := range n.peers {
		n.next[id] = last + 1
		n.match[id] = 0
	}
	n.match[n.id] = last
	n.advanceCommit()
	return n.replicate()
}

func (n *Node) onVote(m Message) Message {
	reply := Message{Type: "voteResp", From: n.id, To: m.From, Term: n.term}
	if m.Term < n.term {
		return reply
	}
	if m.Term > n.term {
		n.follow(m.Term, "")
	}
	if (n.vote == "" || n.vote == m.Candidate) && n.fresh(m.LastIndex, m.LastTerm) {
		n.vote = m.Candidate
		n.role = roleFollower
		n.electN = 0
		_ = n.persist()
		reply.Granted = true
	}
	reply.Term = n.term
	return reply
}

func (n *Node) onVoteResp(m Message) []Message {
	if m.Term > n.term {
		n.follow(m.Term, "")
		return nil
	}
	if n.role != roleCandidate || m.Term != n.term {
		return nil
	}
	n.votes[m.From] = m.Granted
	if n.won() {
		return n.becomeLeader()
	}
	return nil
}

func (n *Node) onAppend(m Message) Message {
	reply := Message{Type: "appendResp", From: n.id, To: m.From, Term: n.term, Hint: n.last()}
	if m.Term < n.term {
		return reply
	}
	n.follow(m.Term, m.Leader)
	reply.Term = n.term
	if m.PrevIndex > n.last() {
		reply.Hint = n.last()
		return reply
	}
	if n.log[m.PrevIndex].Term != m.PrevTerm {
		if m.PrevIndex > 0 {
			reply.Hint = m.PrevIndex - 1
		}
		return reply
	}
	changed := false
	for i := range m.Entries {
		idx := m.PrevIndex + 1 + uint64(i)
		entry := m.Entries[i]
		if idx <= n.last() {
			if n.log[idx].Term == entry.Term {
				continue
			}
			if idx <= n.commit {
				return reply
			}
			n.log = append([]Entry{}, n.log[:idx]...)
			n.log = append(n.log, m.Entries[i:]...)
			changed = true
			break
		}
		n.log = append(n.log, m.Entries[i:]...)
		changed = true
		break
	}
	if changed {
		_ = n.persist()
	}
	if m.Commit > n.commit {
		n.setCommit(m.Commit)
	}
	reply.Success = true
	reply.Hint = n.last()
	reply.Term = n.term
	return reply
}

func (n *Node) onAppendResp(m Message) []Message {
	if m.Term > n.term {
		n.follow(m.Term, "")
		return nil
	}
	if n.role != roleLeader || m.Term != n.term {
		return nil
	}
	if !m.Success {
		next := m.Hint + 1
		if next < n.next[m.From] {
			n.next[m.From] = next
		} else if n.next[m.From] > 1 {
			n.next[m.From]--
		}
		if n.next[m.From] < 1 {
			n.next[m.From] = 1
		}
		return nil
	}
	if m.Hint > n.match[m.From] {
		n.match[m.From] = m.Hint
	}
	if m.Hint+1 > n.next[m.From] {
		n.next[m.From] = m.Hint + 1
	}
	n.advanceCommit()
	return nil
}

func (n *Node) replicate() []Message {
	msgs := make([]Message, 0, len(n.peers))
	for _, id := range n.peers {
		if id == n.id {
			continue
		}
		msgs = append(msgs, n.appendTo(id))
	}
	return msgs
}

func (n *Node) appendTo(id string) Message {
	next := n.next[id]
	if next < 1 {
		next = 1
	}
	if next > n.last()+1 {
		next = n.last() + 1
	}
	prev := next - 1
	entries := append([]Entry{}, n.log[next:]...)
	return Message{
		Type:      "append",
		From:      n.id,
		To:        id,
		Term:      n.term,
		Leader:    n.id,
		PrevIndex: prev,
		PrevTerm:  n.log[prev].Term,
		Entries:   entries,
		Commit:    n.commit,
	}
}

// advanceCommit publishes the highest entry from this term that a majority stored.
// Older entries before that index are published with it.
func (n *Node) advanceCommit() {
	if n.role != roleLeader {
		return
	}
	need := majority(len(n.peers))
	for idx := n.last(); idx > n.commit; idx-- {
		if n.log[idx].Term != n.term {
			continue
		}
		count := 0
		for _, id := range n.peers {
			if n.match[id] >= idx {
				count++
			}
		}
		if count >= need {
			n.setCommit(idx)
			return
		}
	}
}

func (n *Node) setCommit(index uint64) {
	if index > n.last() {
		index = n.last()
	}
	if index <= n.commit {
		return
	}
	n.commit = index
	_ = n.persist()
	n.apply()
}

func (n *Node) apply() {
	for n.applied < n.commit {
		n.applied++
		out := n.sm.apply(n.applied, n.log[n.applied])
		n.results[n.applied] = out
		if ch, ok := n.waiters[n.applied]; ok {
			close(ch)
			delete(n.waiters, n.applied)
		}
	}
}

func (n *Node) follow(term uint64, leader string) {
	wasLeader := n.role == roleLeader
	if term > n.term {
		n.term = term
		n.vote = ""
		_ = n.persist()
		wasLeader = true
	}
	n.role = roleFollower
	if leader != "" {
		n.leader = leader
	}
	n.electN = 0
	if wasLeader {
		n.failWaiters()
	}
}

func (n *Node) failWaiters() {
	for idx, ch := range n.waiters {
		if _, ok := n.results[idx]; ok {
			continue
		}
		n.results[idx] = outcome{index: idx, err: ErrNotLeader}
		close(ch)
		delete(n.waiters, idx)
	}
}

func (n *Node) fresh(index, term uint64) bool {
	last := n.last()
	mine := n.log[last].Term
	if term != mine {
		return term > mine
	}
	return index >= last
}

func (n *Node) won() bool {
	count := 0
	for _, granted := range n.votes {
		if granted {
			count++
		}
	}
	return count >= majority(len(n.peers))
}

func (n *Node) persist() error {
	return saveState(n.dir, stored{
		Term:   n.term,
		Vote:   n.vote,
		Commit: n.commit,
		Log:    n.log,
	})
}

func (n *Node) last() uint64 {
	return uint64(len(n.log) - 1)
}

func majority(n int) int {
	return n/2 + 1
}

func (n *Node) appliedIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.applied
}

func (n *Node) lookup(idx uint64) (outcome, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	out, ok := n.results[idx]
	return out, ok
}

func (n *Node) local(key string) (string, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	v, ok := n.sm.kv[key]
	return v, ok
}
