package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

func main() {
	fmt.Println("==================================================================")
	fmt.Println("  STREAM COMPUTING GRID RAFT CLUSTER COORDINATOR (2014-2018)      ")
	fmt.Println("  Architect: Yousef Khaled <youssef460249@beng.bu.edu.eg>         ")
	fmt.Println("  Consensus: Raft Distributed State Machine                       ")
	fmt.Println("==================================================================")

	peers := []string{"node-2", "node-3", "node-4"}
	node := NewRaftNode("leader-01", peers)

	go func() {
		time.Sleep(500 * time.Millisecond)
		node.StartElection()
	}()

	http.HandleFunc("/cluster/status", func(w http.ResponseWriter, r *http.Request) {
		status := map[string]interface{}{
			"leader_id":     node.nodeID,
			"term":          node.currentTerm,
			"active_peers":  peers,
			"grid_workers":  64,
			"running_tasks": 512,
			"cluster_health": "OPTIMAL",
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	})

	fmt.Println("[COORDINATOR] REST Management API active on :9810")
	log.Fatal(http.ListenAndServe(":9810", nil))
}
