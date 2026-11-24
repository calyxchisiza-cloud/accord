# Accord

Accord keeps one log for a small cluster. A write is visible only after a majority of the nodes have stored it. Keys and locks share that log, so every node that has applied an index agrees on the value and on who holds the lock.

Three nodes in one process print:

```text
color blue
color blue
color blue
payments 3 worker-1
```

Each `color blue` line is the value stored on one node. `payments 3 worker-1` is the lock `payments`, held by `worker-1`, with fencing token 3. The token is the index of the grant. A later grant receives a higher token.

## Run

```powershell
go test ./...
go test -race -count=1 ./...
go run ./cmd/demo
```

Separate processes use loopback ports:

```powershell
go run ./cmd/accord -id a -raft 127.0.0.1:8201 -api 127.0.0.1:8211 -dir data/a
go run ./cmd/accord -id b -raft 127.0.0.1:8202 -api 127.0.0.1:8212 -dir data/b
go run ./cmd/accord -id c -raft 127.0.0.1:8203 -api 127.0.0.1:8213 -dir data/c
```

`GET /v1/status` shows which node is the leader. Send writes and reads to that node's HTTP port.

```powershell
Invoke-RestMethod -Method Put -Uri http://127.0.0.1:8211/v1/kv/color -Body "blue"
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8211/v1/locks/payments -ContentType application/json -Body '{"owner":"worker-1"}'
```

A follower answers `409` and names the leader. A read is itself a log entry, so a node that has lost the majority cannot answer from a stale copy.

The state file for each node is `state.json` in its `-dir`.
