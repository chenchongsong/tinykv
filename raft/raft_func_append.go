package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"sort"
)

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr := r.getProgress(to)
	m := pb.Message{}
	m.To = to

	term, errt := r.RaftLog.Term(pr.Next - 1)
	ents, erre := r.RaftLog.Entries(pr.Next)

	if errt != nil || erre != nil { // send snapshot if we failed to get term or entries
		m.MsgType = pb.MessageType_MsgSnapshot
		snapshot, err := r.RaftLog.snapshot()
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				log.Debugf("%d failed to send snapshot to %d because snapshot is temporarily unavailable", r.id, to)
				return false
			}
			panic(err)
		}
		if IsEmptySnap(&snapshot) {
			panic("need non-empty snapshot")
		}
		m.Snapshot = &snapshot
		sindex, sterm := snapshot.Metadata.Index, snapshot.Metadata.Term
		log.Debugf("%d [firstindex: %d, commit: %d] sent snapshot[index: %d, term: %d] to %d [%v]",
			r.id, r.RaftLog.firstIndex(), r.RaftLog.committed, sindex, sterm, to, pr)
		log.Debugf("%d paused sending replication messages to %d [%v]", r.id, to, pr)
	} else {
		m.MsgType = pb.MessageType_MsgAppend
		m.Index = pr.Next - 1
		m.LogTerm = term

		entries := make([]*pb.Entry, 0, len(ents))
		for i := range ents {
			entries = append(entries, &ents[i])
		}
		m.Entries = entries
		m.Commit = r.RaftLog.committed
	}
	r.send(m)
	return true
}
// 2A
// bcastAppend sends RPC, with entries to all peers that are not up-to-date
// according to the progress recorded in r.Prs.
func (r *Raft) bcastAppend() {
	r.forEachProgress(func(id uint64, _ *Progress) {
		if id == r.id {
			return
		}

		r.sendAppend(id)
	})
}
// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Index < r.RaftLog.committed {
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed})
		return
	}

	ents := make([]pb.Entry, 0, len(m.Entries))
	for _, ent := range m.Entries {
		ents = append(ents, *ent)
	}
	if mlastIndex, ok := r.RaftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, ents...); ok {
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: mlastIndex})
	} else {
		log.Debugf("%d [logterm: %d, index: %d] rejected MessageType_MsgAppend [logterm: %d, index: %d] from %d",
			r.id, r.RaftLog.zeroTermOnRangeErr(r.RaftLog.Term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: m.Index, Reject: true, RejectHint: r.RaftLog.LastIndex()})
	}
}
// 2A
func (r *Raft) appendEntry(es ...pb.Entry) {
	li := r.RaftLog.LastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
	}
	// use latest "last" index after truncate/append
	li = r.RaftLog.append(es...)
	r.getProgress(r.id).maybeUpdate(li)
	// Regardless of maybeCommit's return, our caller will call bcastAppend.
	r.maybeCommit()
}


// 2A
// maybeUpdate returns false if the given n index comes from an outdated message.
// Otherwise it updates the progress and returns true.
func (pr *Progress) maybeUpdate(n uint64) bool {
	var updated bool
	if pr.Match < n {
		pr.Match = n
		updated = true
	}
	if pr.Next < n+1 {
		pr.Next = n + 1
	}
	return updated
}
// 2A
// maybeCommit attempts to advance the commit index. Returns true if
// the commit index changed (in which case the caller should call
// r.bcastAppend).
func (r *Raft) maybeCommit() bool {
	matchIndex := make(uint64Slice, len(r.Prs))
	idx := 0
	for _, p := range r.Prs {
		matchIndex[idx] = p.Match
		idx++
	}
	sort.Sort(matchIndex)
	mci := matchIndex[len(matchIndex)-r.quorum()]
	return r.RaftLog.maybeCommit(mci, r.Term)
}
