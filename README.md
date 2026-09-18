# Raft - A Learning Implementation in Go (WIP)

This is a naive implementation of the [raft](https://raft.github.io/raft.pdf) algorithm in Golang for learning purpose.
This is a Work In Progress state.

## Run
To run a single node:
```base
go run .
```

To run multiple nodes and monitor the logs:
```bash
./raft-nodes-start.sh
```
And after a moment kill the script and look at the logs on `raft-combined.log.out`

## Design

The protocol is event driven. No code waits for a reply inside the same
call. Messages are one way: a request is sent, and the reply arrives
later as its own event.

* `Step(from, msg)` handles one message
* `Tick()` moves logical time (election and heartbeat timeouts)
* `Serve()` is the production driver: it only feeds `Step` and `Tick`
* `Transport` sends messages; gRPC is one implementation

This shape makes the node testable in a deterministic simulator, and it
removes the liveness risk of blocking on a slow peer.

## Usage

### 1. Write (Submit)

All writes must go through the **leader**.

#### Request example

```json
{
  "command": "SET X 10",
  "serial_number": "1"
}
```

#### Flow

1. Send request to any node
2. If not leader → you get:

```json
   {
     "success": false,
     "leader_id": "node1"
   }
```
3. Retry on leader
4. Leader:

   * appends log
   * replicates to quorum
   * commits
   * applies to state machine
   * replies to the client when the commit index passes the entry
     (the request is not answered inside the call)

#### Response

```json
{
  "success": true
}
```

### 2. Read (Get)

Reads must also go through the **leader**.

#### Request example

```json
{
  "command": "GET X"
}
```

#### Flow

1. Send request
2. If not leader → redirect
3. Leader:

   * sends a heartbeat round (it does not wait for the replies yet)
   * reads from state machine

> Note: the leader does not confirm its leadership before reading,
> so a partitioned leader can return stale data. ReadIndex is not
> implemented yet.

#### Response

```json
{
  "status": true,
  "value": "10"
}
```

### 3. Leader Redirection

Both `Submit` and `Get` may return:

```json
{
  "success": false,
  "leader_id": "node1"
}
```

Client must retry on the leader.

### 4. Membership Change (Joint Consensus)

Cluster changes use **two-phase joint consensus**.

#### Add Node

```json
{
  "add_ids": ["node2"],
  "add_nodes": {
    "node2": "localhost:50052"
  }
}
```

#### Remove Node

```json
{
  "remove_ids": ["node2"]
}
```

#### Add + Remove Together

```json
{
  "add_ids": ["node3"],
  "add_nodes": {
    "node3": "localhost:50053"
  },
  "remove_ids": ["node2"]
}
```

### Membership Change Flow

**Phase 1 (Joint Config)**

* Cluster enters `C_old,new`
* Both configs must agree (dual quorum)

**Phase 2 (Final Config)**

* New config committed
* Old nodes removed
* Cluster stabilizes

## Example Full Workflow

1. Start cluster
2. Wait for leader election
3. Write:

```json
{
  "command": "SET Y 16",
  "serial_number": "122"
}
```

4. Read:

```json
{
  "command": "GET Y"
}
```

5. Add node:

```json
{
  "add_ids": ["node2"],
  "add_nodes": {
    "node2": "localhost:50052"
  }
}
```

6. Remove node:

```json
{
  "remove_ids": ["node2"]
}
```

## Reference
* Raft Paper: [https://raft.github.io/raft.pdf](https://raft.github.io/raft.pdf)
