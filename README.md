# Replicated Agent Memory Store

A leaderless, fully replicated memory layer for LLM agents. Facts are stored as
versioned key-value records on every node. The point of the project is not
storage, but what happens when two agent sessions write contradicting facts and
what the cluster does when the network splits.

It is a teaching system, small enough to read. Not Dynamo, not Raft.

## Features

- Leaderless quorum replication (N=3, configurable to 5)
- Tunable per-request read and write quorums (R, W)
- Two conflict policies: last-write-wins and CRDTs (LWW-register, OR-Set)
- Read repair, hinted handoff, and anti-entropy for convergence
- Crash-stop and network-partition tolerance, with a deterministic fault injector
- In-memory store plus a per-node write-ahead log

## How it works

Every node holds the whole keyspace. A coordinator forwards each write to all
replicas and returns once W acks arrive. A read waits for R responses and
resolves them.

Consistency is tunable. When R + W > N the read and write quorums overlap, so a
read after a successful write returns that write. When R + W <= N a read may
return stale data. The system exposes this tradeoff instead of choosing for you.

Versions are `(lamport, node_id)`, giving a total order. Wall-clock time is
accepted but ignored for ordering, so clock skew cannot invert causality.

Conflicts are resolved per key by policy, set on first write:

- **LWW** for scalar facts like `customer:plan`. The highest version wins.
- **CRDT LWW-register** for the same intent expressed as a mergeable payload.
- **CRDT OR-Set** for set-valued facts like `user:tags`, where a concurrent add
  and remove must not lose the add.

Split-brain is allowed. Both sides of a partition may accept writes their local
W permits. After the partition heals, LWW or the CRDT merge picks a value and
the cluster converges.

## Install

```bash
go test -race ./...
```

## Usage

Run a single node:

```bash
go run ./cmd/node -id A -http :8081 -peers A=127.0.0.1:8081
```

Put and get a fact with the CLI:

```bash
go run ./cmd/memctl -addr http://127.0.0.1:8081 put-fact -entity customer-1 -attr plan -value enterprise
go run ./cmd/memctl -addr http://127.0.0.1:8081 get-fact -entity customer-1 -attr plan
```

Run a 3-node Docker cluster with the conflict demo:

```bash
docker compose up -d --build
bash scripts/demo.sh
bash scripts/partition_demo.sh
```

## Stack

Go, HTTP/JSON API, in-memory store with a write-ahead log, Docker Compose for
the cluster, `go test` for the suite.
