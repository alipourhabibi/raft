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

## Problems
- [ ] the RequestVote does not return asas possible
- [ ] Check for the order of the heartbeat and etc in methods
