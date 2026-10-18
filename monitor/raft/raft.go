// Package raft replicates monitor commands (encoded transitions.Event) across monitors.
// No snapshots, no log compaction, no membership changes. Storage failures are fatal.
//
// Leadership is lease-based (check-quorum): a leader is valid only while a majority
// acknowledged it within ElectionTimeout, and a follower that heard from a leader
// within ElectionTimeout refuses votes. So a deposed leader stops answering before
// another one can be elected, which the nodes' serve gate relies on.
package raft

import (
	"context"
	"errors"
	"log"
	"math/rand/v2"
	"net/http"
	"slices"
	"sync"
	"time"

	"mycelium/core/wire"
)

var ErrNotLeader = errors.New("not leader")

const maxBatch = 256 // entries per append request

type Config struct {
	ID                string
	Peers             map[string]string // id -> base URL, including self
	Dir               string
	ElectionTimeout   time.Duration // minimum; the actual timeout is randomized in [1x, 2x)
	HeartbeatInterval time.Duration
	Applied           uint64 // index already reflected in the persisted map
}

type role int

const (
	follower role = iota
	candidate
	leader
)

type waiter struct {
	term uint64
	ch   chan error
}

type Raft struct {
	cfg      Config
	client   *wire.Client
	storage  *storage
	apply    func(index uint64, cmd []byte) error
	onLeader func()
	quorum   int
	applyCh  chan struct{}
	kicks    map[string]chan struct{} // per other peer

	mu            sync.Mutex
	role          role
	hs            hardState
	log           []wire.Entry // log[i].Index == i+1
	commitIndex   uint64
	lastApplied   uint64
	nextIndex     map[string]uint64
	matchIndex    map[string]uint64
	ackSent       map[string]time.Time // leader: send time of the latest acked request per peer
	leaderContact time.Time            // last time a current leader was heard from
	leaderSince   time.Time
	deadline      time.Time // election deadline when not leader
	readyIndex    uint64    // leader: index of its no-op entry
	ready         bool      // leader and readyIndex applied
	stopLeader    context.CancelFunc
	waiters       map[uint64]waiter
}

// New loads persisted state. apply is called in log order for every committed
// command above cfg.Applied; its error is returned by the matching Propose.
// onLeader is called each time this monitor becomes leader with all prior entries applied.
func New(cfg Config, c *wire.Client, apply func(index uint64, cmd []byte) error, onLeader func()) (*Raft, error) {
	if _, ok := cfg.Peers[cfg.ID]; !ok {
		return nil, errors.New("raft: own id missing from peers")
	}
	st, hs, es, err := openStorage(cfg.Dir)
	if err != nil {
		return nil, err
	}
	if cfg.Applied > uint64(len(es)) {
		return nil, errors.New("raft: persisted map is ahead of the log")
	}
	r := &Raft{
		cfg:         cfg,
		client:      c,
		storage:     st,
		apply:       apply,
		onLeader:    onLeader,
		quorum:      len(cfg.Peers)/2 + 1,
		applyCh:     make(chan struct{}, 1),
		kicks:       map[string]chan struct{}{},
		hs:          hs,
		log:         es,
		commitIndex: cfg.Applied,
		lastApplied: cfg.Applied,
		nextIndex:   map[string]uint64{},
		matchIndex:  map[string]uint64{},
		ackSent:     map[string]time.Time{},
		// A restart counts as having just heard from a leader, so this monitor
		// cannot help elect a new one while an old leader's lease may still hold.
		leaderContact: time.Now(),
		waiters:       map[uint64]waiter{},
	}
	for id := range cfg.Peers {
		if id != cfg.ID {
			r.kicks[id] = make(chan struct{}, 1)
		}
	}
	r.resetDeadline()
	return r, nil
}

// Run drives election timeouts, leadership checks and applying, until ctx is done.
func (r *Raft) Run(ctx context.Context) {
	go r.applyCommitted(ctx)
	t := time.NewTicker(r.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			r.mu.Lock()
			r.becomeFollower(r.hs.Term)
			r.mu.Unlock()
			return
		case <-t.C:
		}
		r.mu.Lock()
		now := time.Now()
		switch {
		case r.role == leader:
			if now.Sub(r.leaderSince) > r.cfg.ElectionTimeout && !r.leaseValid(now) {
				r.becomeFollower(r.hs.Term)
			}
		case now.After(r.deadline):
			r.campaign(ctx)
		}
		r.mu.Unlock()
	}
}

// Propose appends cmd and returns once it is committed and applied locally,
// with the apply error. ErrNotLeader means the outcome is unknown.
func (r *Raft) Propose(ctx context.Context, cmd []byte) error {
	r.mu.Lock()
	if !r.isLeader(time.Now()) {
		r.mu.Unlock()
		return ErrNotLeader
	}
	e := wire.Entry{Term: r.hs.Term, Index: r.lastIndex() + 1, Cmd: cmd}
	r.must(r.storage.append([]wire.Entry{e}))
	r.log = append(r.log, e)
	ch := make(chan error, 1)
	r.waiters[e.Index] = waiter{term: e.Term, ch: ch}
	r.advanceCommit()
	r.mu.Unlock()
	r.kickAll()

	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		r.mu.Lock()
		delete(r.waiters, e.Index)
		r.mu.Unlock()
		return ctx.Err()
	}
}

// IsLeader reports whether this monitor is the leader, holds a valid lease and
// has applied every entry of previous terms.
func (r *Raft) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.isLeader(time.Now())
}

// Handler serves POST /raft/vote and POST /raft/append.
func (r *Raft) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /raft/vote", r.handleVote)
	mux.HandleFunc("POST /raft/append", r.handleAppend)
	return mux
}

func (r *Raft) handleVote(w http.ResponseWriter, req *http.Request) {
	var q wire.VoteReq
	if err := wire.ReadJSON(req, &q); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	reject := wire.VoteReply{Term: r.hs.Term}
	if q.Term < r.hs.Term {
		wire.WriteJSON(w, http.StatusOK, reject)
		return
	}
	// Leader stickiness: ignore candidates while a leader is known to be alive.
	if (r.role == leader && r.leaseValid(now)) ||
		(r.role == follower && now.Sub(r.leaderContact) < r.cfg.ElectionTimeout) {
		wire.WriteJSON(w, http.StatusOK, reject)
		return
	}
	if q.Term > r.hs.Term {
		r.becomeFollower(q.Term)
	}
	lastIndex := r.lastIndex()
	lastTerm := r.termAt(lastIndex)
	upToDate := q.LastLogTerm > lastTerm || (q.LastLogTerm == lastTerm && q.LastLogIndex >= lastIndex)
	if (r.hs.VotedFor == "" || r.hs.VotedFor == q.Candidate) && upToDate {
		r.hs.VotedFor = q.Candidate
		r.must(r.storage.saveHardState(r.hs))
		r.resetDeadline()
		wire.WriteJSON(w, http.StatusOK, wire.VoteReply{Term: r.hs.Term, Granted: true})
		return
	}
	wire.WriteJSON(w, http.StatusOK, wire.VoteReply{Term: r.hs.Term})
}

func (r *Raft) handleAppend(w http.ResponseWriter, req *http.Request) {
	var q wire.AppendReq
	if err := wire.ReadJSON(req, &q); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if q.Term < r.hs.Term {
		wire.WriteJSON(w, http.StatusOK, wire.AppendReply{Term: r.hs.Term})
		return
	}
	if q.Term > r.hs.Term || r.role != follower {
		r.becomeFollower(q.Term)
	}
	r.leaderContact = time.Now()
	r.resetDeadline()

	last := r.lastIndex()
	if q.PrevLogIndex > last || r.termAt(q.PrevLogIndex) != q.PrevLogTerm {
		// MatchIndex carries a hint: the leader retries from hint+1.
		hint := last
		if q.PrevLogIndex <= last {
			hint = q.PrevLogIndex - 1
		}
		wire.WriteJSON(w, http.StatusOK, wire.AppendReply{Term: r.hs.Term, MatchIndex: hint})
		return
	}
	for i, e := range q.Entries {
		if e.Index <= r.lastIndex() {
			if r.termAt(e.Index) == e.Term {
				continue
			}
			r.log = r.log[:e.Index-1]
			r.must(r.storage.rewrite(r.log))
		}
		r.must(r.storage.append(q.Entries[i:]))
		r.log = append(r.log, q.Entries[i:]...)
		break
	}
	match := q.PrevLogIndex + uint64(len(q.Entries))
	if c := min(q.LeaderCommit, match); c > r.commitIndex {
		r.commitIndex = c
		r.signalApply()
	}
	wire.WriteJSON(w, http.StatusOK, wire.AppendReply{Term: r.hs.Term, Success: true, MatchIndex: match})
}

// campaign starts an election for the next term. Requires r.mu.
func (r *Raft) campaign(ctx context.Context) {
	r.role = candidate
	r.hs = hardState{Term: r.hs.Term + 1, VotedFor: r.cfg.ID}
	r.must(r.storage.saveHardState(r.hs))
	r.resetDeadline()
	term := r.hs.Term
	votes := 1
	if votes >= r.quorum {
		r.becomeLeader(ctx)
		return
	}
	lastIndex := r.lastIndex()
	q := wire.VoteReq{Term: term, Candidate: r.cfg.ID, LastLogIndex: lastIndex, LastLogTerm: r.termAt(lastIndex)}
	for id, url := range r.cfg.Peers {
		if id == r.cfg.ID {
			continue
		}
		go func() {
			cctx, cancel := context.WithTimeout(ctx, r.cfg.ElectionTimeout)
			defer cancel()
			var rep wire.VoteReply
			if st, err := r.client.DoJSON(cctx, http.MethodPost, url+"/raft/vote", q, &rep); err != nil || st != http.StatusOK {
				return
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if rep.Term > r.hs.Term {
				r.becomeFollower(rep.Term)
				return
			}
			if r.role != candidate || r.hs.Term != term || !rep.Granted {
				return
			}
			if votes++; votes >= r.quorum {
				r.becomeLeader(ctx)
			}
		}()
	}
}

// replicate sends appends (heartbeats included) to one peer for one leader term.
func (r *Raft) replicate(ctx context.Context, peer string, term uint64) {
	url := r.cfg.Peers[peer]
	for {
		r.mu.Lock()
		if ctx.Err() != nil || r.role != leader || r.hs.Term != term {
			r.mu.Unlock()
			return
		}
		next := r.nextIndex[peer]
		end := min(uint64(len(r.log)), next-1+maxBatch)
		q := wire.AppendReq{
			Term:         term,
			Leader:       r.cfg.ID,
			PrevLogIndex: next - 1,
			PrevLogTerm:  r.termAt(next - 1),
			Entries:      slices.Clone(r.log[next-1 : end]),
			LeaderCommit: r.commitIndex,
		}
		r.mu.Unlock()

		sent := time.Now()
		cctx, cancel := context.WithTimeout(ctx, r.cfg.ElectionTimeout)
		var rep wire.AppendReply
		st, err := r.client.DoJSON(cctx, http.MethodPost, url+"/raft/append", q, &rep)
		cancel()

		more := false
		if err == nil && st == http.StatusOK {
			r.mu.Lock()
			if rep.Term > r.hs.Term {
				r.becomeFollower(rep.Term)
				r.mu.Unlock()
				return
			}
			if r.role == leader && r.hs.Term == term {
				if sent.After(r.ackSent[peer]) {
					r.ackSent[peer] = sent
				}
				if rep.Success {
					r.matchIndex[peer] = max(r.matchIndex[peer], rep.MatchIndex)
					r.nextIndex[peer] = r.matchIndex[peer] + 1
					r.advanceCommit()
				} else {
					r.nextIndex[peer] = rep.MatchIndex + 1
				}
				more = r.nextIndex[peer] <= r.lastIndex()
			}
			r.mu.Unlock()
		}
		if more {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-r.kicks[peer]:
		case <-time.After(r.cfg.HeartbeatInterval):
		}
	}
}

// advanceCommit commits the highest current-term entry stored on a quorum. Requires r.mu.
func (r *Raft) advanceCommit() {
	for n := r.lastIndex(); n > r.commitIndex && r.termAt(n) == r.hs.Term; n-- {
		count := 1
		for _, m := range r.matchIndex {
			if m >= n {
				count++
			}
		}
		if count >= r.quorum {
			r.commitIndex = n
			r.signalApply()
			return
		}
	}
}

// applyCommitted applies committed entries in order and resolves waiters, until ctx is done.
func (r *Raft) applyCommitted(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.applyCh:
		}
		for {
			r.mu.Lock()
			if r.lastApplied >= r.commitIndex {
				r.mu.Unlock()
				break
			}
			e := r.log[r.lastApplied]
			r.mu.Unlock()

			var err error
			if e.Cmd != nil { // nil = a leader's no-op
				err = r.apply(e.Index, e.Cmd)
			}

			r.mu.Lock()
			r.lastApplied = e.Index
			if w, ok := r.waiters[e.Index]; ok {
				delete(r.waiters, e.Index)
				if w.term != e.Term {
					err = ErrNotLeader
				}
				w.ch <- err
			}
			ready := r.role == leader && !r.ready && e.Index == r.readyIndex && e.Term == r.hs.Term
			r.ready = r.ready || ready
			r.mu.Unlock()
			if ready && r.onLeader != nil {
				r.onLeader()
			}
		}
	}
}

// becomeLeader requires r.mu. It appends a no-op so entries of earlier terms commit.
func (r *Raft) becomeLeader(ctx context.Context) {
	now := time.Now()
	r.role = leader
	r.ready = false
	r.leaderSince = now
	last := r.lastIndex()
	for id := range r.kicks {
		r.nextIndex[id] = last + 1
		r.matchIndex[id] = 0
		r.ackSent[id] = time.Time{}
	}
	noop := wire.Entry{Term: r.hs.Term, Index: last + 1}
	r.must(r.storage.append([]wire.Entry{noop}))
	r.log = append(r.log, noop)
	r.readyIndex = noop.Index

	lctx, cancel := context.WithCancel(ctx)
	r.stopLeader = cancel
	for id := range r.kicks {
		go r.replicate(lctx, id, r.hs.Term)
	}
	r.advanceCommit()
	log.Printf("raft: %s is leader for term %d", r.cfg.ID, r.hs.Term)
}

// becomeFollower requires r.mu. Pending proposals fail with ErrNotLeader.
func (r *Raft) becomeFollower(term uint64) {
	if term > r.hs.Term {
		r.hs = hardState{Term: term}
		r.must(r.storage.saveHardState(r.hs))
	}
	if r.role != follower {
		r.resetDeadline()
	}
	r.role = follower
	r.ready = false
	if r.stopLeader != nil {
		r.stopLeader()
		r.stopLeader = nil
	}
	for i, w := range r.waiters {
		delete(r.waiters, i)
		w.ch <- ErrNotLeader
	}
}

// isLeader requires r.mu.
func (r *Raft) isLeader(now time.Time) bool {
	return r.role == leader && r.ready && r.leaseValid(now)
}

// leaseValid: a quorum (self included) acked a request sent less than ElectionTimeout ago. Requires r.mu.
func (r *Raft) leaseValid(now time.Time) bool {
	times := []time.Time{now}
	for id := range r.kicks {
		times = append(times, r.ackSent[id])
	}
	slices.SortFunc(times, func(a, b time.Time) int { return b.Compare(a) })
	return now.Sub(times[r.quorum-1]) < r.cfg.ElectionTimeout
}

func (r *Raft) resetDeadline() {
	et := r.cfg.ElectionTimeout
	r.deadline = time.Now().Add(et + rand.N(et))
}

func (r *Raft) lastIndex() uint64 { return uint64(len(r.log)) }

func (r *Raft) termAt(i uint64) uint64 {
	if i == 0 {
		return 0
	}
	return r.log[i-1].Term
}

func (r *Raft) signalApply() {
	select {
	case r.applyCh <- struct{}{}:
	default:
	}
}

func (r *Raft) kickAll() {
	for _, ch := range r.kicks {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (r *Raft) must(err error) {
	if err != nil {
		log.Fatalf("raft: storage: %v", err)
	}
}
