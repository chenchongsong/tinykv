// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"strings"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}


type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1].
	randomizedElectionTimeout int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	PendingConfIndex uint64

	// 2A
	// number of ticks since it reached last electionTimeout
	electionElapsed int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raftlog := newLog(c.Storage)
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	peers := c.peers
	if len(cs.Nodes) > 0 {
		if len(peers) > 0 {
			panic("cannot specify both newRaft (peers) and ConfState.(Nodes)")
		}
		peers = cs.Nodes
	}
	r := &Raft{
		id:               c.ID,
		Lead:             None,
		RaftLog:          raftlog,
		Prs:              make(map[uint64]*Progress),
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
	}
	for _, p := range peers {
		r.Prs[p] = &Progress{Next: 1}
	}

	if !IsEmptyHardState(hs) {
		r.loadState(hs)
	}
	if c.Applied > 0 {
		raftlog.appliedTo(c.Applied)
	}
	r.becomeFollower(r.Term, None)

	var nodesStrs []string
	for _, n := range nodes(r) {
		nodesStrs = append(nodesStrs, fmt.Sprintf("%d", n))
	}
	log.Infof("newRaft %d [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
		r.id, strings.Join(nodesStrs, ","), r.Term, r.RaftLog.committed, r.RaftLog.applied, r.RaftLog.LastIndex(), r.RaftLog.lastTerm())
	return r
}

// 2A
func (r *Raft) quorum() int { return len(r.Prs)/2 + 1 }

// 2A
// send persists state to stable storage and then sends to its mailbox.
func (r *Raft) send(m pb.Message) {
	m.From = r.id
	if m.MsgType == pb.MessageType_MsgRequestVote || m.MsgType == pb.MessageType_MsgRequestVoteResponse {
		if m.Term == 0 {
			// All campaign messages need to have the term set when sending.
			// - MessageType_MsgRequestVote: m.Term is the term the node is campaigning for,
			//   non-zero as we increment the term when campaigning.
			// - MessageType_MsgRequestVoteResponse: m.Term is the new r.Term if the MessageType_MsgRequestVote was
			//   granted, non-zero for the same reason MessageType_MsgRequestVote is
			panic(fmt.Sprintf("term should be set when sending %s", m.MsgType))
		}
	} else {
		if m.Term != 0 {
			panic(fmt.Sprintf("term should not be set when sending %s (was %d)", m.MsgType, m.Term))
		}
		// do not attach term to MessageType_MsgPropose
		// proposals are a way to forward to the leader and
		// should be treated as local message.
		if m.MsgType != pb.MessageType_MsgPropose {
			m.Term = r.Term
		}
	}
	r.msgs = append(r.msgs, m)
}


// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}
// 2A
func (r *Raft) getProgress(id uint64) *Progress {
	return r.Prs[id]
}
// 2A
func (r *Raft) forEachProgress(f func(id uint64, pr *Progress)) {
	for id, pr := range r.Prs {
		f(id, pr)
	}
}
// 2A
func (r *Raft) setProgress(id, match, next uint64) {
	r.Prs[id] = &Progress{Next: next, Match: match}
	return
}


// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here 2A
	switch r.State {
	case StateFollower, StateCandidate:
		r.tickElection()
	case StateLeader:
		r.tickHeartbeat()
	}
}
// 2A
// tickElection is run by followers and candidates after r.electionTimeout.
func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.promotable() && r.pastElectionTimeout() {
		r.electionElapsed = 0
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
	}
}
// 2A
// tickHeartbeat is run by leaders to send a MessageType_MsgBeat after r.heartbeatTimeout.
func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++

	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		// If current leader cannot transfer leadership in electionTimeout, it becomes leader again.
		if r.State == StateLeader && r.leadTransferee != None {
			r.abortLeaderTransfer()
		}
	}
	if r.State != StateLeader {
		return
	}
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgBeat})
	}
}


// 2A
func (r *Raft) loadState(state pb.HardState) {
	if state.Commit < r.RaftLog.committed || state.Commit > r.RaftLog.LastIndex() {
		log.Panicf("%d state.commit %d is out of range [%d, %d]", r.id, state.Commit, r.RaftLog.committed, r.RaftLog.LastIndex())
	}
	r.RaftLog.committed = state.Commit
	r.Term = state.Term
	r.Vote = state.Vote
}
// 2AC
// softState return the softState of this peer
func (r *Raft) softState() *SoftState {
	return &SoftState{Lead: r.Lead, RaftState: r.State}
}
// 2AC
// hardState return the hardState of this peer
func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

// 2AC
func (r *Raft) GetSnap() *pb.Snapshot {
	return r.RaftLog.pendingSnapshot
}
// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	sindex, sterm := m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term
	if r.restore(*m.Snapshot) {
		log.Infof("%d [commit: %d] restored snapshot [index: %d, term: %d]",
			r.id, r.RaftLog.committed, sindex, sterm)
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.LastIndex()})
	} else {
		log.Infof("%d [commit: %d] ignored snapshot [index: %d, term: %d]",
			r.id, r.RaftLog.committed, sindex, sterm)
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed})
	}
}
// 2B
func (r *Raft) restore(s pb.Snapshot) bool {
	if s.Metadata.Index <= r.RaftLog.committed {
		return false
	}
	if r.RaftLog.matchTerm(s.Metadata.Index, s.Metadata.Term) {
		log.Infof("%d [commit: %d, lastindex: %d, lastterm: %d] fast-forwarded commit to snapshot [index: %d, term: %d]",
			r.id, r.RaftLog.committed, r.RaftLog.LastIndex(), r.RaftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term)
		r.RaftLog.commitTo(s.Metadata.Index)
		return false
	}

	log.Infof("%d [commit: %d, lastindex: %d, lastterm: %d] starts to restore snapshot [index: %d, term: %d]",
		r.id, r.RaftLog.committed, r.RaftLog.LastIndex(), r.RaftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term)

	r.RaftLog.restore(s)
	r.Prs = make(map[uint64]*Progress)
	r.restoreNode(s.Metadata.ConfState.Nodes)
	return true
}

// 2B
func (r *Raft) restoreNode(nodes []uint64) {
	for _, n := range nodes {
		match, next := uint64(0), r.RaftLog.LastIndex()+1
		if n == r.id {
			match = next - 1
		}
		r.setProgress(n, match, next)
		log.Infof("%d restored progress of %d [%+v]", r.id, n, r.getProgress(n))
	}
}
// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}
// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
