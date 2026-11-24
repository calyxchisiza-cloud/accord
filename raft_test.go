package accord

import (
	"errors"
	"testing"
)

func testCluster(t *testing.T, ids ...string) *Cluster {
	t.Helper()
	c, err := Start(t.TempDir(), ids)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func TestElectionHasOneLeader(t *testing.T) {
	c := testCluster(t, "a", "b", "c")
	if err := c.Campaign("a"); err != nil {
		t.Fatal(err)
	}
	if c.Leader() != "a" {
		t.Fatalf("leader %s", c.Leader())
	}
	if err := c.Campaign("b"); err != nil {
		t.Fatal(err)
	}
	leaders := 0
	for _, id := range c.ids {
		if c.nodes[id].Status().Role == roleLeader {
			leaders++
		}
	}
	if leaders != 1 || c.Leader() != "b" {
		t.Fatalf("leaders %d current %s", leaders, c.Leader())
	}
}

func TestPutReachesEveryNode(t *testing.T) {
	c := testCluster(t, "a", "b", "c")
	if err := c.Campaign("a"); err != nil {
		t.Fatal(err)
	}
	if err := c.Put("color", "blue"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b", "c"} {
		got, err := c.Get(id, "color")
		if err != nil || got != "blue" {
			t.Fatalf("%s %q %v", id, got, err)
		}
	}
}

func TestStoppedLeaderKeepsCommittedKeys(t *testing.T) {
	c := testCluster(t, "a", "b", "c")
	if err := c.Campaign("a"); err != nil {
		t.Fatal(err)
	}
	if err := c.Put("color", "blue"); err != nil {
		t.Fatal(err)
	}
	c.Stop("a")
	if err := c.Campaign("b"); err != nil {
		t.Fatal(err)
	}
	got, err := c.Get("b", "color")
	if err != nil || got != "blue" {
		t.Fatalf("after election %q %v", got, err)
	}
	if err := c.Put("extra", "1"); err != nil {
		t.Fatal(err)
	}
	if err := c.Restart("a"); err != nil {
		t.Fatal(err)
	}
	got, err = c.Get("a", "color")
	if err != nil || got != "blue" {
		t.Fatalf("restarted color %q %v", got, err)
	}
	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	got, err = c.Get("a", "extra")
	if err != nil || got != "1" {
		t.Fatalf("restarted extra %q %v", got, err)
	}
}

func TestIsolatedLeaderCannotPublish(t *testing.T) {
	c := testCluster(t, "a", "b", "c")
	if err := c.Campaign("a"); err != nil {
		t.Fatal(err)
	}
	if err := c.Put("color", "blue"); err != nil {
		t.Fatal(err)
	}
	c.Isolate("a")
	_, msgs, err := c.nodes["a"].Propose(Entry{Kind: "put", Key: "color", Value: "red"})
	if err != nil {
		t.Fatal(err)
	}
	c.send(msgs)
	if err := c.until(func() bool { return false }); err != ErrNoQuorum {
		t.Fatalf("isolated put %v", err)
	}
	got, err := c.Get("b", "color")
	if err != nil || got != "blue" {
		t.Fatalf("majority still %q %v", got, err)
	}
}

func TestUncommittedEntryIsReplaced(t *testing.T) {
	c := testCluster(t, "a", "b", "c")
	if err := c.Campaign("a"); err != nil {
		t.Fatal(err)
	}
	c.Isolate("a")
	_, msgs, err := c.nodes["a"].Propose(Entry{Kind: "put", Key: "color", Value: "red"})
	if err != nil {
		t.Fatal(err)
	}
	c.send(msgs)
	if err := c.Campaign("b"); err != nil {
		t.Fatal(err)
	}
	if err := c.Put("color", "green"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get("a", "color"); err != ErrNotFound {
		t.Fatalf("uncommitted read %v", err)
	}
	c.Heal("a")
	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	for _, id := range c.ids {
		got, err := c.Get(id, "color")
		if err != nil || got != "green" {
			t.Fatalf("%s %q %v", id, got, err)
		}
	}
	leaders := 0
	for _, id := range c.ids {
		if c.nodes[id].Status().Role == roleLeader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("leaders %d", leaders)
	}
}

func TestLockTokenAndHolder(t *testing.T) {
	c := testCluster(t, "a", "b", "c")
	if err := c.Campaign("a"); err != nil {
		t.Fatal(err)
	}
	first, err := c.Lock("payments", "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Lock("payments", "worker-2"); !errors.Is(err, ErrHeld) {
		t.Fatalf("second holder %v", err)
	}
	again, err := c.Lock("payments", "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	if again <= first {
		t.Fatalf("token %d then %d", first, again)
	}
	if err := c.Unlock("payments", "worker-2"); !errors.Is(err, ErrNotHeld) {
		t.Fatalf("wrong owner %v", err)
	}
	if err := c.Unlock("payments", "worker-1"); err != nil {
		t.Fatal(err)
	}
	token, err := c.Lock("payments", "worker-2")
	if err != nil || token <= again {
		t.Fatalf("new holder %d %v", token, err)
	}
}

func TestRestartReadsCommittedValue(t *testing.T) {
	dir := t.TempDir()
	c, err := Start(dir, []string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Campaign("a"); err != nil {
		t.Fatal(err)
	}
	if err := c.Put("color", "blue"); err != nil {
		t.Fatal(err)
	}
	c.Close()

	c, err = Start(dir, []string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	got, err := c.Get("b", "color")
	if err != nil || got != "blue" {
		t.Fatalf("reloaded %q %v", got, err)
	}
	if _, err := c.Get("b", "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing %v", err)
	}
}

func TestSingleNodeCommits(t *testing.T) {
	c := testCluster(t, "a")
	if err := c.Campaign("a"); err != nil {
		t.Fatal(err)
	}
	if err := c.Put("color", "blue"); err != nil {
		t.Fatal(err)
	}
	got, err := c.Get("a", "color")
	if err != nil || got != "blue" {
		t.Fatalf("%q %v", got, err)
	}
}

func TestInvalidName(t *testing.T) {
	c := testCluster(t, "a")
	if err := c.Campaign("a"); err != nil {
		t.Fatal(err)
	}
	if err := c.Put("Color", "blue"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("%v", err)
	}
}
