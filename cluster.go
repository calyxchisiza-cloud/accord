package accord

import "path/filepath"

// Cluster is three or more nodes in one process.
// Messages move when the cluster drains them, so a test can pause the network.
type Cluster struct {
	dir      string
	ids      []string
	nodes    map[string]*Node
	down     map[string]bool
	isolated map[string]bool
	queue    []Message
}

// Start opens one durable node per id under dir.
func Start(dir string, ids []string) (*Cluster, error) {
	if len(ids) == 0 {
		return nil, ErrInvalid
	}
	c := &Cluster{
		dir:      dir,
		ids:      append([]string{}, ids...),
		nodes:    map[string]*Node{},
		down:     map[string]bool{},
		isolated: map[string]bool{},
	}
	for _, id := range c.ids {
		node, err := Open(filepath.Join(dir, id), id, c.ids)
		if err != nil {
			c.Close()
			return nil, err
		}
		c.nodes[id] = node
	}
	return c, nil
}

// Close stops every node.
func (c *Cluster) Close() {
	for _, id := range c.ids {
		if c.nodes[id] != nil {
			c.nodes[id].Stop()
		}
	}
}

// Leader is the connected leader with the highest term.
func (c *Cluster) Leader() string {
	var found string
	var term uint64
	for _, id := range c.ids {
		if c.down[id] || c.isolated[id] {
			continue
		}
		status := c.nodes[id].Status()
		if status.Role == roleLeader && status.Term >= term {
			found = id
			term = status.Term
		}
	}
	return found
}

// Campaign starts an election on id and waits until a majority applies its blank entry.
func (c *Cluster) Campaign(id string) error {
	if c.down[id] || c.isolated[id] || c.nodes[id] == nil {
		return ErrNoQuorum
	}
	c.nodes[id].Force()
	c.send(c.nodes[id].Tick())
	return c.until(func() bool {
		if c.nodes[id].Status().Role != roleLeader {
			return false
		}
		return c.quorumApplied(c.nodes[id].appliedIndex())
	})
}

// Put writes a key after a majority has stored the command.
func (c *Cluster) Put(key, value string) error {
	_, err := c.command(Entry{Kind: "put", Key: key, Value: value})
	return err
}

// Delete removes a key after a majority has stored the command.
func (c *Cluster) Delete(key string) error {
	_, err := c.command(Entry{Kind: "del", Key: key})
	return err
}

// Lock grants name to owner and returns a fencing token.
// The token is the log index of the grant. A later grant has a higher token.
func (c *Cluster) Lock(name, owner string) (uint64, error) {
	out, err := c.command(Entry{Kind: "lock", Key: name, Owner: owner})
	if err != nil {
		return 0, err
	}
	return out.token, nil
}

// Unlock releases name when owner holds it.
func (c *Cluster) Unlock(name, owner string) error {
	_, err := c.command(Entry{Kind: "unlock", Key: name, Owner: owner})
	return err
}

// Get reads the value id has applied.
func (c *Cluster) Get(id, key string) (string, error) {
	node := c.nodes[id]
	if node == nil || c.down[id] {
		return "", ErrClosed
	}
	if err := validName(key); err != nil {
		return "", err
	}
	value, ok := node.local(key)
	if !ok {
		return "", ErrNotFound
	}
	return value, nil
}

// Isolate drops every message to or from id.
func (c *Cluster) Isolate(id string) {
	c.isolated[id] = true
}

// Heal lets id send and receive again.
func (c *Cluster) Heal(id string) {
	delete(c.isolated, id)
}

// Stop freezes id. Its files stay on disk.
func (c *Cluster) Stop(id string) {
	c.down[id] = true
	if c.nodes[id] != nil {
		c.nodes[id].Stop()
	}
}

// Restart loads id from disk as a follower.
func (c *Cluster) Restart(id string) error {
	if old := c.nodes[id]; old != nil {
		old.Stop()
	}
	node, err := Open(filepath.Join(c.dir, id), id, c.ids)
	if err != nil {
		return err
	}
	c.nodes[id] = node
	delete(c.down, id)
	return nil
}

// Sync waits until every connected node has applied the leader's log.
func (c *Cluster) Sync() error {
	return c.until(func() bool {
		leader := c.Leader()
		if leader == "" {
			return false
		}
		target := c.nodes[leader].appliedIndex()
		if target == 0 {
			return false
		}
		return c.allReached(target)
	})
}

func (c *Cluster) command(e Entry) (outcome, error) {
	if err := validate(e); err != nil {
		return outcome{}, err
	}
	leader := c.Leader()
	if leader == "" {
		return outcome{}, ErrNotLeader
	}
	idx, msgs, err := c.nodes[leader].Propose(e)
	if err != nil {
		return outcome{}, err
	}
	c.send(msgs)
	if err := c.until(func() bool {
		_, ok := c.nodes[leader].lookup(idx)
		return ok
	}); err != nil {
		return outcome{}, err
	}
	out, _ := c.nodes[leader].lookup(idx)
	if out.err == ErrNotLeader {
		return outcome{}, ErrNotLeader
	}
	if err := c.until(func() bool { return c.allReached(idx) }); err != nil {
		return outcome{}, err
	}
	return out, out.err
}

func (c *Cluster) until(ready func() bool) error {
	for i := 0; i < 400; i++ {
		c.drain()
		if ready() {
			return nil
		}
		if leader := c.Leader(); leader != "" {
			c.send(c.nodes[leader].Tick())
		}
	}
	return ErrNoQuorum
}

func (c *Cluster) quorumApplied(idx uint64) bool {
	if idx == 0 {
		return false
	}
	count := 0
	for _, id := range c.ids {
		if c.down[id] {
			continue
		}
		if c.nodes[id].appliedIndex() >= idx {
			count++
		}
	}
	return count >= majority(len(c.ids))
}

func (c *Cluster) allReached(idx uint64) bool {
	for _, id := range c.ids {
		if c.down[id] || c.isolated[id] {
			continue
		}
		if c.nodes[id].appliedIndex() < idx {
			return false
		}
	}
	return true
}

func (c *Cluster) send(msgs []Message) {
	for _, msg := range msgs {
		if msg.To == "" || msg.To == msg.From {
			continue
		}
		c.queue = append(c.queue, msg)
	}
}

func (c *Cluster) drain() {
	for len(c.queue) > 0 {
		msg := c.queue[0]
		c.queue = c.queue[1:]
		if c.blocked(msg.From, msg.To) {
			continue
		}
		node := c.nodes[msg.To]
		if node == nil {
			continue
		}
		c.send(node.Handle(msg))
	}
}

func (c *Cluster) blocked(from, to string) bool {
	if c.down[from] || c.down[to] {
		return true
	}
	if c.isolated[from] || c.isolated[to] {
		return true
	}
	return false
}
