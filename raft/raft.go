package raft

import (
	"net/rpc"
	"sync"
)

type State int

const (
	Follower = iota
	Candidate
	Leader
)

type Node struct {
	mu sync.Mutex

	id    int
	peers map[int]string // id -> host:port, doesnot include selfs

	state       State
	currentTerm int
	voteFor     int

	rpcServer *rpc.Server
}
