package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Raft consensus state machine engineered by Yousef Khaled (2014-2018)

type NodeRole int

const (
	Follower NodeRole = iota
	Candidate
	Leader
)

type LogEntry struct {
	Index int64  `json:"index"`
	Term  int64  `json:"term"`
	Data  string `json:"data"`
}

type RaftNode struct {
	mu          sync.Mutex
	nodeID      string
	currentTerm int64
	votedFor    string
	role        NodeRole
	log         []LogEntry
	commitIndex int64
	lastApplied int64
	peers       []string
}

func NewRaftNode(id string, peers []string) *RaftNode {
	return &RaftNode{
		nodeID:      id,
		currentTerm: 1,
		role:        Follower,
		log:         make([]LogEntry, 0),
		peers:       peers,
	}
}

func (rn *RaftNode) StartElection() {
	rn.mu.Lock()
	rn.role = Candidate
	rn.currentTerm++
	rn.votedFor = rn.nodeID
	term := rn.currentTerm
	rn.mu.Unlock()

	fmt.Printf("[RAFT] Node %s transitioned to Candidate for Term %d\n", rn.nodeID, term)

	// Simulate quorum election
	time.Sleep(time.Duration(150+rand.Intn(150)) * time.Millisecond)

	rn.mu.Lock()
	if rn.role == Candidate {
		rn.role = Leader
		fmt.Printf("[RAFT] Node %s elected as LEADER for Term %d (Quorum: %d/%d)\n", 
			rn.nodeID, rn.currentTerm, len(rn.peers)/2+1, len(rn.peers)+1)
	}
	rn.mu.Unlock()
}
