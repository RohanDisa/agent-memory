# Replicated Agent Memory Store

A teaching-grade, **leaderless** memory layer for LLM agents. Facts are versioned
key-value records, **fully replicated** on every node. The interesting problem is
not storage — it is what happens when two agent sessions write contradicting facts
about the same entity, and what the cluster does when the network splits.

This is not Dynamo and not Raft. It is small enough to read, and every distributed
claim below is pinned to a test that induces the condition with `faultnet`.

```
N = 3  (configurable to 5)
replication = quorum, coordinator = whichever node got the client request
ordering    = Lamport clock + node_id   (never wall clock)
failures    = crash-stop + network partition   (not Byzantine)
```

---

## The (R, W) consistency matrix at N=3

`R + W > N` means the read and write quorums **intersect**, so a read after a
successful write returns that write (read-your-writes, per key). `R + W ≤ N`
does **not** intersect; a read is allowed to return stale data. The system
exposes the tradeoff. It does not pick one for you.

| R \ W | W=1 | W=2 | W=3 |
|---|---|---|---|
| **R=1** | eventual. `1+1 ≤ 3`. Stale read **demonstrated**. | eventual. `1+2 = 3 ≰ intersect`. No RYW guarantee. | **RYW**. `1+3 > 3`. |
| **R=2** | eventual. `2+1 = 3`. No RYW guarantee. | **RYW**. `2+2 > 3`. | **RYW**. |
| **R=3** | **RYW**. `3+1 > 3`. | **RYW**. | **RYW**. Slowest / least available. |

Evidence:

- Matrix sweep on a healthy cluster: `TestRWMatrixN3`
- RYW holds when `R+W>N`: `TestReadYourWritesWhenRPlusWGreaterThanN` (R=2, W=2)
- RYW does **not** hold when `R+W≤N`: `TestReadYourWritesDoesNotHoldWhenRPlusWAtMostN`
  (isolate C, write W=1 on A, read R=1 on C → miss)

Write availability with one node down (N=3):

| W | 1 node down | test |
|---|---|---|
| 2 | write **succeeds** | `TestQuorumWriteOneNodeDownW2SucceedsW3Fails` |
| 3 | write **fails** | same |

---

## One partition scenario

`faultnet.Partition({A,B}, {C})` — `TestPartitionMajorityWritesSucceedMinorityStaleThenHealConverges` (seed=11)

1. Majority `{A,B}` accepts writes at **W=2**.
2. Minority `{C}` rejects W=2 (only one ack is possible).
3. C is stale on the majority key.
4. `Heal()` + two anti-entropy rounds → C has the majority value.
5. Convergence in the test process: **two version-scan rounds** (measured in
   `TestMeasureThroughputAndPartitionAvailability`).

Split-brain is **allowed**. Both sides may accept writes that their local W
permits. After heal, LWW or the CRDT merge picks a value. The LWW loser is
discarded by design. That is the honest answer to "how do you handle
split-brain?": we don't prevent it; we converge afterward.

---

## Why this exists

Agent memory is usually one Redis or one vector DB. That is a single point of
failure, and it silently overwrites when two sessions disagree. This project
takes the disagreement seriously: replication, tunable quorum consistency,
and two conflict policies you can defend in an interview.

It is a teaching system. It demonstrates quorum replication, tunable
consistency, LWW vs CRDT merge, hinted handoff, read repair, anti-entropy, and
behavior under partition — each with a test. It does not try to be Dynamo.

---

## What it is / is not

| It is | It is not |
|---|---|
| Leaderless quorum replication | Consensus (no Raft/Paxos) |
| Full replica on every node | Sharded / partitioned keyspace |
| At-least-once + idempotent apply | Exactly-once |
| Tunable per-request R, W | Globally strong, or cross-key transactions |
| Crash-stop + partitions | Byzantine-tolerant |
| In-memory + per-node WAL | Durable across a **full-cluster** restart (stretch) |
| Tests + a tiny demo script | An LLM product (Ollama is optional garnish) |

---

## Topology and versioning

- **N nodes**, static membership (`-peers A=host:port,B=...,C=...`).
- Every node holds the **whole** keyspace. Isolating consistency from sharding
  is the point.
- A coordinator forwards a write to all N replicas and returns at **W** acks.
  A read waits for **R** responses and resolves them.
- Versions are `(lamport, node_id)`. Comparison is a total order: higher
  lamport wins; equal lamport breaks ties by `node_id`. Wall-clock time is
  accepted as `client_ts` and **ignored for ordering** — clock skew would
  otherwise invert causality. Total-order tests:
  `TestCompareAntisymmetric`, `TestCompareTransitive`, `TestCompareNodeIDTiebreak`.

Replication is **at-least-once**. `Replica.Apply` is idempotent by version
(CRDT keys merge). Never "exactly-once."

---

## Conflict resolution (the agent-memory part)

Policy is per key, set on first write.

| Policy | For | Rule | Test |
|---|---|---|---|
| **LWW** | scalar latest-value facts (`customer:plan`) | max `(lamport, node_id)` wins; older write is dropped | `TestLWWConcurrentWritesEveryReplicaAgrees` |
| **CRDT LWW-register** | same intent, as a CvRDT | `Merge` is associative, commutative, idempotent | 300 ACI trials + 200 random orderings in `internal/conflict` |
| **CRDT OR-Set** | set-valued facts (`user:tags`) | add-wins: a remove only tombstones **observed** tags | `TestORSetAddWinsConcurrentAddRemove`, 250 ACI + 300 fold orderings |

**Which facts suit which policy.** Latest-known scalars (plan, preferred
channel, last-shipped date) → LWW / LWW-register. Sets (tags, permissions,
known issues) → OR-Set, because concurrent `add(x)` and `remove(x)` must not
lose the add.

**Limitation a data structure cannot fix.** `enterprise` vs `free` is a
*domain* contradiction. LWW picks one; it does not know which is true. The
store can flag "replicas disagreed" (`replicas_agreed=false`) but it cannot
invent a semantic resolver. That is an application problem.

The difference between LWW and CRDT LWW-register: LWW is "keep the highest
coordinator-assigned version." The CRDT is a payload whose `Merge` is a
mathematical operation we property-test over hundreds of random states. Same
intent, different claim.

---

## Convergence machinery

| Mechanism | What it does | Required test |
|---|---|---|
| **Read repair** | On a disagreeing read, push the winner to stale respondents | `TestReadRepairConvergesForcedStaleReplica` — inject stale C, read R=3, inspect C |
| **Hinted handoff** | Unreachable replica → coordinator stores a hint, replays on recover | `TestHintedHandoffDeliversToRecoveredNode` |
| **Anti-entropy** | Pairwise version-scan (not Merkle; Merkle is stretch) | `TestAntiEntropyConvergesWithReadsDisabled` — reads **off** so read repair cannot cheat |

---

## Fault injector

`internal/net/faultnet.go` sits under the in-process transport:

```
Partition(left, right)   // symmetric: both directions die
Heal()
Drop(from, to, p)        // applied to both directions
Delay(from, to, d)
Isolate(id) / Recover(id)
FailFromAfter(id, n)     // mid-write isolation, deterministic
```

Deterministic under a seed. A failing chaos test is replayable. Partitions
are symmetric (`TestPartitionIsSymmetric`).

Docker uses the same node handlers plus a per-process deny-list
(`POST /debug/fault`) so `scripts/partition_demo.sh` can split the compose
cluster without iptables.

---

## Agent API

Thin wrapper, no consistency of its own.

```
put_fact(entity, attribute, value, policy)  → key "entity:attribute"
get_fact(entity, attribute, R)
get_facts(entity)                           → local prefix scan (not a coordinated range)
add_tag / remove_tag                        → OR-Set on "entity:tags"
get_history(entity, attribute)              → WAL
```

---

## Claim → test index

Every "survives X" / consistency sentence in this README maps here. No test, no claim.

| Claim | Test |
|---|---|
| W=2 survives 1 down; W=3 does not | `TestQuorumWriteOneNodeDownW2SucceedsW3Fails` |
| RYW iff R+W>N | `TestReadYourWritesWhenRPlusWGreaterThanN`, `TestReadYourWritesDoesNotHoldWhenRPlusWAtMostN`, `TestRWMatrixN3` |
| Read repair converges a stale replica | `TestReadRepairConvergesForcedStaleReplica` |
| Hinted handoff delivers after recover | `TestHintedHandoffDeliversToRecoveredNode` |
| Anti-entropy converges with reads disabled | `TestAntiEntropyConvergesWithReadsDisabled` |
| Concurrent LWW writes → one winner on every replica | `TestConcurrentConflictingWritesConverge`, `TestLWWConcurrentWritesEveryReplicaAgrees` |
| Partition majority writes; heal converges | `TestPartitionMajorityWritesSucceedMinorityStaleThenHealConverges` |
| Isolate coordinator mid-write → clean client failure | `TestIsolateCoordinatorMidWriteCleanFailure` |
| 30% drops, then converge | `TestDrop30PercentThenConverge` |
| Majority write availability during partition | `TestWriteAvailabilityDuringPartition` |
| Tombstones replicate (deletes are not "absence") | `TestTombstoneReplicatesAndIsNotUndone` |
| Version order is total | `TestCompareAntisymmetric`, `TestCompareTransitive` |
| WAL replay | `TestWALReplayFromDisk` |
| CRDT laws (property tests, not examples) | `TestLWWRegCommutativeAssociativeIdempotent`, `TestORSetMergeIdempotentCommutativeAssociative`, `TestORSetIdenticalOpSetsConverge` |
| OR-Set add-wins after partition | `TestORSetConcurrentAddRemoveAddWinsOnEveryReplica` |

Chaos tests are seeded (`Seed: 11, 22, 33, 44`).

---

## Measurements

Collected by `TestMeasureThroughputAndPartitionAvailability` and the chaos
tests on an in-process 3-node cluster (laptop, no Docker). Re-run
`go test ./test/integration ./test/chaos -count=1 -v` to refresh.

| Metric | Observed in-process on this repo's test run |
|---|---|
| Heal → converge | **2 anti-entropy rounds**, 2.1ms in `TestMeasureThroughputAndPartitionAvailability` |
| Majority write availability during `{A,B}\|{C}` | **W=1 15/15, W=2 15/15, W=3 0/15** (`TestWriteAvailabilityDuringPartition`); same pattern 20/20 and 0/20 in the measure test |
| CRDT LWW-reg random orderings | **200** + 300 ACI trials, all converging |
| CRDT OR-Set random fold orderings | **300** + 250 ACI trials, all converging |
| In-process write cost | tens of microseconds per write at W=1/2/3 (no real network). R=3 reads wait for every replica; R=1/R=2 returned under the timer resolution in the same run. |

These numbers characterize the **in-process** harness (the suite that CI
runs). They are not a production load test.

---

## Run

```bash
# unit + integration + chaos (CI). No Docker.
go test -race -count=1 -coverprofile=coverage.out ./...
go tool cover -func=coverage.out | tail -1

# 3-node Docker cluster + live conflict demo
docker compose up -d --build
bash scripts/demo.sh
bash scripts/partition_demo.sh

# optional: Ollama phrases facts. No test depends on this.
bash scripts/ollama_demo.sh
```

```bash
# single process (N=1)
go run ./cmd/node -id A -http :8081 -peers A=127.0.0.1:8081

# memctl
go run ./cmd/memctl -addr http://127.0.0.1:8081 put-fact -entity customer-1 -attr plan -value enterprise
go run ./cmd/memctl -addr http://127.0.0.1:8081 get-fact -entity customer-1 -attr plan
```

**CI** (`/.github/workflows/ci.yml`): unit + integration + chaos on every
push. Docker e2e (`go test -tags e2e ./test/e2e`) is manual / nightly.

**Coverage:** `go test -coverpkg=./internal/... -coverprofile=coverage.out ./...`
then `go tool cover -func=coverage.out`. Last measured **85.6% of statements
in `internal/...`** (unit + integration + chaos; 72 `Test*` functions, including
the 9-cell `TestRWMatrixN3` subtests). `make cover` reproduces the number.
The race detector runs in CI on Linux; it needs CGO and a 64-bit compiler.

---

## Stack

| Concern | Choice |
|---|---|
| Language | Go |
| Schema | `proto/memory.proto` (IDL) |
| In-process RPC | `faultnet` calling the same handlers |
| Process RPC + client API | HTTP/JSON matching that schema (zero codegen; curl works) |
| Storage | in-memory map + append-only WAL |
| Cluster | Docker Compose, one container per node |
| Tests | `go test` + in-process harness; Docker for e2e |
| Lint | golangci-lint |


## Layout

See the tree under `internal/`, `test/integration`, `test/chaos`, `test/e2e`,
`cmd/node`, `cmd/memctl`, `scripts/`. Unit tests sit next to the package they
cover (`internal/.../*_test.go`); `test/unit` is a pointer, not a second copy.
