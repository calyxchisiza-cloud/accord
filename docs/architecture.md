# Architecture

Accord is a three-node cluster that replicates one log. The algorithm is Raft: terms, leader election, and a commit rule that waits for a majority.

## Election

A follower that hears nothing starts an election, increments its term, and votes for itself. It asks the others for a vote. A peer grants that vote when the candidate's log is at least as long as its own, and only if it has not already voted in that term. A majority of votes makes a leader. One term has one winner. A message from a higher term makes the receiver a follower.

On winning, the leader appends a blank entry. That entry is from the new term, which is what allows older entries to become committed.

## Commit

The leader tracks how far each peer has stored the log. It advances the commit index to the highest entry that a majority has stored, and only when that entry's term is the leader's current term. Every earlier entry is committed with it.

A follower applies an entry only after the leader's commit index includes it. Apply is in order, and it rebuilds a key-value map and the lock table. Two holders cannot both own a lock at the same index: the second command is stored in the log and every node applies it as a refusal.

A node that is cut off from the other two can still believe it is the leader. It cannot move the commit index, because a majority has not stored the new entry. When it can talk to the cluster again, the majority log overwrites any entry that was never committed.

## Disk

Each node keeps its term, its vote, its commit index, and its log in `state.json`. The new file is synced, then renamed into place. A crash leaves either the previous file or `state.json.bak`. On open, the node applies the committed prefix and leaves any uncommitted tail unapplied.

There is no snapshot and no membership change. The log is capped at 10,000 entries.

## Two ways to run it

The demo and the tests put three nodes in one process and pass messages by hand. That makes a partition and a restart deterministic.

`cmd/accord` is a separate process. Raft messages are length-prefixed JSON over TCP. The HTTP API on the loopback address is how an application puts a key, reads it, or takes a lock. Listen addresses have to be loopback.
