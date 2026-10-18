// Package wire holds the request/reply types, status codes and an HTTP client helper.
package wire

import "mycelium/core/mapmodel"

const (
	StatusFenced     = 409 // holder: epoch < fence
	StatusNotPrimary = 421 // primary: not primary for this PG (monitor: not leader)
	StatusInactive   = 503 // primary: PG inactive
)

// Headers carrying shard metadata and the epoch on binary shard routes.
const (
	HeaderEpoch   = "X-Epoch"
	HeaderVersion = "X-Version"
	HeaderSize    = "X-Size"
	HeaderSha256  = "X-Sha256"
	HeaderDeleted = "X-Deleted"
)

// Shard is one shard record, or a tombstone when Deleted is set.
// Size is the whole object's size; Sha256 covers Data.
type Shard struct {
	ID      string
	Version mapmodel.Version
	Size    int64
	Sha256  [32]byte
	Deleted bool
	Data    []byte
}

type IDVersion struct {
	ID      string           `json:"id"`
	Version mapmodel.Version `json:"version"`
}

type IDVersions struct {
	ID       string             `json:"id"`
	Versions []mapmodel.Version `json:"versions"`
}

// Monitor routes.

type HeartbeatReq struct {
	NodeID mapmodel.NodeID `json:"node_id"`
	BootID string          `json:"boot_id"`
}

type JoinReq struct {
	NodeID mapmodel.NodeID `json:"node_id"`
	BootID string          `json:"boot_id"`
	Addr   string          `json:"addr"`
}

// NodeReply answers both heartbeat and join.
type NodeReply struct {
	Epoch mapmodel.Epoch `json:"epoch"`
	State mapmodel.State `json:"state"`
}

type BackfillDoneReq struct {
	Epoch  mapmodel.Epoch  `json:"epoch"`
	NodeID mapmodel.NodeID `json:"node_id"`
}

// Holder routes.

// BatchReq is the body of commit and discard.
type BatchReq struct {
	Epoch mapmodel.Epoch `json:"epoch"`
	Items []IDVersion    `json:"items"`
}

type ScanReq struct {
	Epoch mapmodel.Epoch `json:"epoch"`
}

type LookupReq struct {
	Epoch mapmodel.Epoch `json:"epoch"`
	IDs   []string       `json:"ids"`
}

// IDVersionsReply answers scan and lookup.
type IDVersionsReply struct {
	Items []IDVersions `json:"items"`
}

// FenceReply is the body of a 409.
type FenceReply struct {
	Fence mapmodel.Epoch `json:"fence"`
}

// Raft routes.

type Entry struct {
	Term  uint64 `json:"term"`
	Index uint64 `json:"index"`
	Cmd   []byte `json:"cmd"`
}

type VoteReq struct {
	Term         uint64 `json:"term"`
	Candidate    string `json:"candidate"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

type VoteReply struct {
	Term    uint64 `json:"term"`
	Granted bool   `json:"granted"`
}

type AppendReq struct {
	Term         uint64  `json:"term"`
	Leader       string  `json:"leader"`
	PrevLogIndex uint64  `json:"prev_log_index"`
	PrevLogTerm  uint64  `json:"prev_log_term"`
	Entries      []Entry `json:"entries"`
	LeaderCommit uint64  `json:"leader_commit"`
}

type AppendReply struct {
	Term       uint64 `json:"term"`
	Success    bool   `json:"success"`
	MatchIndex uint64 `json:"match_index"`
}
