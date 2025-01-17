package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here 2A
	// Handle the message term, which may result in our stepping down to a follower.
	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.Term:
		log.Infof("%d [term: %d] received a %s message with higher term from %d [term: %d]",
			r.id, r.Term, m.MsgType, m.From, m.Term)
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	case m.Term < r.Term:
		log.Infof("%d [term: %d] ignored a %s message with lower term from %d [term: %d]", r.id, r.Term, m.MsgType, m.From, m.Term)
		return nil
	}

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if r.State != StateLeader {
			ents, err := r.RaftLog.slice(r.RaftLog.applied+1, r.RaftLog.committed+1)
			if err != nil {
				log.Panicf("unexpected error getting unapplied entries (%v)", err)
			}
			if n := numOfPendingConf(ents); n != 0 && r.RaftLog.committed > r.RaftLog.applied {
				log.Warningf("%d cannot campaign at term %d since there are still %d pending configuration changes to apply", r.id, r.Term, n)
				return nil
			}

			log.Infof("%d is starting a new election at term %d", r.id, r.Term)

			r.campaign()
		} else {
			log.Debugf("%d ignoring MessageType_MsgHup because already leader", r.id)
		}
	case pb.MessageType_MsgRequestVote:
		// We can vote if this is a repeat of a vote we've already cast...
		canVote := r.Vote == m.From ||
			// ...we haven't voted and we don't think there's a leader yet in this term...
			(r.Vote == None && r.Lead == None)
		// ...and we believe the candidate is up to date.
		if canVote && r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
			log.Infof("%d [logterm: %d, index: %d, vote: %d] cast %s for %d [logterm: %d, index: %d] at term %d",
				r.id, r.RaftLog.lastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Term: m.Term, MsgType: pb.MessageType_MsgRequestVoteResponse})
			// Only record real votes.
			r.electionElapsed = 0
			r.Vote = m.From
		} else {
			log.Infof("%d [logterm: %d, index: %d, vote: %d] rejected %s from %d [logterm: %d, index: %d] at term %d",
				r.id, r.RaftLog.lastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
		}
	default:
		switch r.State {
		case StateFollower:
			err := r.stepFollower(m)
			if err != nil {
				return err
			}
		case StateCandidate:
			err := r.stepCandidate(m)
			if err != nil {
				return err
			}
		case StateLeader:
			err := r.stepLeader(m)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// 2A
// stepLeader handle leader's message
func (r *Raft) stepLeader(m pb.Message) error {
	// Your Code Here 2A
	pr := r.getProgress(m.From)
	if pr == nil && m.MsgType != pb.MessageType_MsgBeat && m.MsgType != pb.MessageType_MsgPropose {
		log.Debugf("%d no progress available for %d", r.id, m.From)
		return nil
	}

	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
		return nil
	case pb.MessageType_MsgPropose:
		if len(m.Entries) == 0 {
			log.Panicf("%d stepped empty MessageType_MsgPropose", r.id)
		}
		if _, ok := r.Prs[r.id]; !ok {
			// If we are not currently a member of the range (i.e. this node
			// was removed from the configuration while serving as leader),
			// drop any new proposals.
			return ErrProposalDropped
		}
		if r.leadTransferee != None {
			log.Debugf("%d [term %d] transfer leadership to %d is in progress; dropping proposal", r.id, r.Term, r.leadTransferee)
			return ErrProposalDropped
		}

		for i, e := range m.Entries {
			if e.EntryType == pb.EntryType_EntryConfChange {
				if r.PendingConfIndex > r.RaftLog.applied {
					log.Infof("propose conf %s ignored since pending unapplied configuration [index %d, applied %d]",
						e.String(), r.PendingConfIndex, r.RaftLog.applied)
					m.Entries[i] = &pb.Entry{EntryType: pb.EntryType_EntryNormal}
				} else {
					r.PendingConfIndex = r.RaftLog.LastIndex() + uint64(i) + 1
				}
			}
		}

		es := make([]pb.Entry, 0, len(m.Entries))
		for _, e := range m.Entries {
			es = append(es, *e)
		}

		r.appendEntry(es...)
		r.bcastAppend()
		return nil
	case pb.MessageType_MsgAppendResponse:
		if m.Reject {
			log.Debugf("%d received MessageType_MsgAppend rejection(lastindex: %d) from %d for index %d",
				r.id, m.RejectHint, m.From, m.Index)
			if pr.maybeDecrTo(m.Index, m.RejectHint) {
				r.sendAppend(m.From)
			}
		} else {
			if pr.maybeUpdate(m.Index) {

				if r.maybeCommit() {
					r.bcastAppend()
				}
				// Transfer leadership is in progress.
				if m.From == r.leadTransferee && pr.Match == r.RaftLog.LastIndex() {
					log.Infof("%d sent MessageType_MsgTimeoutNow to %d after received MessageType_MsgAppendResponse", r.id, m.From)
					r.sendTimeoutNow(m.From)
				}
			}
		}
	case pb.MessageType_MsgHeartbeatResponse:
		if pr.Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgTransferLeader:
		leadTransferee := m.From
		lastLeadTransferee := r.leadTransferee
		if lastLeadTransferee != None {
			if lastLeadTransferee == leadTransferee {
				log.Infof("%d [term %d] transfer leadership to %d is in progress, ignores request to same node %d",
					r.id, r.Term, leadTransferee, leadTransferee)
				return nil
			}
			r.abortLeaderTransfer()
			log.Infof("%d [term %d] abort previous transferring leadership to %d", r.id, r.Term, lastLeadTransferee)
		}
		if leadTransferee == r.id {
			log.Debugf("%d is already leader. Ignored transferring leadership to self", r.id)
			return nil
		}
		// Transfer leadership to third party.
		log.Infof("%d [term %d] starts to transfer leadership to %d", r.id, r.Term, leadTransferee)
		// Transfer leadership should be finished in one electionTimeout, so reset r.electionElapsed.
		r.electionElapsed = 0
		r.leadTransferee = leadTransferee
		if pr.Match == r.RaftLog.LastIndex() {
			r.sendTimeoutNow(leadTransferee)
			log.Infof("%d sends MessageType_MsgTimeoutNow to %d immediately as %d already has up-to-date log", r.id, leadTransferee, leadTransferee)
		} else {
			r.sendAppend(leadTransferee)
		}
	}
	return nil
}

// 2A
// stepCandidate handle candidate's message
func (r *Raft) stepCandidate(m pb.Message) error {
	// Your Code Here 2A
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		log.Infof("%d no leader at term %d; dropping proposal", r.id, r.Term)
		return ErrProposalDropped
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleSnapshot(m)
	case pb.MessageType_MsgRequestVoteResponse:
		gr := r.poll(m.From, m.MsgType, !m.Reject)
		log.Infof("%d [quorum:%d] has received %d %s votes and %d vote rejections", r.id, r.quorum(), gr, m.MsgType, len(r.votes)-gr)
		switch r.quorum() {
		case gr:
			r.becomeLeader()
			r.bcastAppend()
		case len(r.votes) - gr:
			// m.Term > r.Term; reuse r.Term
			r.becomeFollower(r.Term, None)
		}
	case pb.MessageType_MsgTimeoutNow:
		log.Debugf("%d [term %d state %v] ignored MessageType_MsgTimeoutNow from %d", r.id, r.Term, r.State, m.From)
	}
	return nil
}

// 2A
// stepFollower handle follower's message
func (r *Raft) stepFollower(m pb.Message) error {
	// Your Code Here 2A
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		log.Infof("%d is no leader at term %d; dropping proposal", r.id, r.Term)
		return ErrProposalDropped
	case pb.MessageType_MsgAppend:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		if r.Lead == None {
			log.Infof("%d no leader at term %d; dropping leader transfer msg", r.id, r.Term)
			return nil
		}
		m.To = r.Lead
		r.send(m)
	case pb.MessageType_MsgTimeoutNow:
		if r.promotable() {
			log.Infof("%d [term %d] received MessageType_MsgTimeoutNow from %d and starts an election to get leadership.", r.id, r.Term, m.From)
			r.campaign()
		} else {
			log.Infof("%d received MessageType_MsgTimeoutNow from %d but is not promotable", r.id, m.From)
		}
	}
	return nil
}
// 2A
// promotable indicates whether state machine can be promoted to Leader,
// which is true when its own id is in progress list.
func (r *Raft) promotable() bool {
	_, ok := r.Prs[r.id]
	return ok
}


// maybeDecrTo returns false if the given to index comes from an out of order message.
// Otherwise it decreases the progress next index to min(rejected, last) and returns true.
func (pr *Progress) maybeDecrTo(rejected, last uint64) bool {
	// the rejection must be stale if the progress has matched and "rejected"
	// is smaller than "match".
	if rejected <= pr.Match {
		return false
	}
	if pr.Next = min(rejected, last+1); pr.Next < 1 {
		pr.Next = 1
	}
	return true
}


func numOfPendingConf(ents []pb.Entry) int {
	n := 0
	for i := range ents {
		if ents[i].EntryType == pb.EntryType_EntryConfChange {
			n++
		}
	}
	return n
}