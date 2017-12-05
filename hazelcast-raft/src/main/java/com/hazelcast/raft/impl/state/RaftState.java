package com.hazelcast.raft.impl.state;

import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.log.RaftLog;
import com.hazelcast.raft.impl.operation.ChangeRaftGroupMembershipOp.MembershipChangeType;

import java.util.Collection;
import java.util.HashSet;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftState {

    private final RaftEndpoint localEndpoint;
    private final String name;
    private RaftGroupMembers committedGroupMembers;
    private RaftGroupMembers lastGroupMembers;

    private RaftRole role = RaftRole.FOLLOWER;
    private int term;

    private volatile RaftEndpoint leader;

    // index of highest committed log entry
    private int commitIndex;

    // index of highest log entry that's applied to state
    // lastApplied <= commitIndex
    private int lastApplied;

    private RaftEndpoint votedFor;
    private int lastVoteTerm;

    private RaftLog log = new RaftLog();

    private LeaderState leaderState;
    private CandidateState preCandidateState;
    private CandidateState candidateState;

    public RaftState(String name, RaftEndpoint localEndpoint, Collection<RaftEndpoint> endpoints) {
        assert endpoints.contains(localEndpoint)
                : "Members set must contain local member! Members: " + endpoints + ", Local member: " + localEndpoint;
        this.name = name;
        this.localEndpoint = localEndpoint;
        RaftGroupMembers groupMembers = new RaftGroupMembers(0, endpoints, localEndpoint);
        this.committedGroupMembers = groupMembers;
        this.lastGroupMembers = groupMembers;
    }

    public String name() {
        return name;
    }

    public Collection<RaftEndpoint> members() {
        return lastGroupMembers.members();
    }

    public Collection<RaftEndpoint> remoteMembers() {
        return lastGroupMembers.remoteMembers();
    }

    public int memberCount() {
        return lastGroupMembers.memberCount();
    }

    public int majority() {
        return lastGroupMembers.majority();
    }

    public int membersLogIndex() {
        return lastGroupMembers.index();
    }

    public RaftRole role() {
        return role;
    }

    public int term() {
        return term;
    }

    int incrementTerm() {
        return ++term;
    }

    public RaftEndpoint leader() {
        return leader;
    }

    public int lastVoteTerm() {
        return lastVoteTerm;
    }

    public RaftEndpoint votedFor() {
        return votedFor;
    }

    public void leader(RaftEndpoint endpoint) {
        leader = endpoint;
    }

    public int commitIndex() {
        return commitIndex;
    }

    public void commitIndex(int index) {
        assert index >= commitIndex : "new commit index: " + index + " is smaller than current commit index: " + commitIndex;
        commitIndex = index;
    }

    public int lastApplied() {
        return lastApplied;
    }

    public void lastApplied(int index) {
        assert index >= lastApplied : "new last applied: " + index + " is smaller than current last applied: " + lastApplied;
        lastApplied = index;
    }

    public RaftLog log() {
        return log;
    }

    public LeaderState leaderState() {
        return leaderState;
    }

    public CandidateState candidateState() {
        return candidateState;
    }

    public void persistVote(int term, RaftEndpoint endpoint) {
        this.lastVoteTerm = term;
        this.votedFor = endpoint;
    }

    public void toFollower(int term) {
        role = RaftRole.FOLLOWER;
        leader = null;
        preCandidateState = null;
        leaderState = null;
        candidateState = null;
        this.term = term;
    }

    public VoteRequest toCandidate() {
        role = RaftRole.CANDIDATE;
        preCandidateState = null;
        leaderState = null;
        candidateState = new CandidateState(majority());
        candidateState.grantVote(localEndpoint);
        persistVote(incrementTerm(), localEndpoint);

        return new VoteRequest(localEndpoint, term, log.lastLogOrSnapshotTerm(), log.lastLogOrSnapshotIndex());
    }

    public void toLeader() {
        role = RaftRole.LEADER;
        leader(localEndpoint);
        preCandidateState = null;
        candidateState = null;
        leaderState = new LeaderState(lastGroupMembers.remoteMembers(), log.lastLogOrSnapshotIndex());
    }

    public boolean isKnownEndpoint(RaftEndpoint endpoint) {
        return lastGroupMembers.isKnownEndpoint(endpoint);
    }

    public void initPreCandidateState() {
        preCandidateState = new CandidateState(majority());
        preCandidateState.grantVote(localEndpoint);
    }

    public CandidateState preCandidateState() {
        return preCandidateState;
    }

    public void updateGroupMembers(int logIndex, RaftEndpoint endpoint, MembershipChangeType changeType) {
        assert committedGroupMembers == lastGroupMembers
                : "Cannot make group members update: " + changeType + " for " + endpoint + " at log index: " + logIndex
                + " because last group members: " + lastGroupMembers + " is different than committed group members: "
                + committedGroupMembers;
        assert lastGroupMembers.index() < logIndex
                : "Cannot make group members update: " + changeType + " for " + endpoint + " at log index: " + logIndex
                + " because last group members: " + lastGroupMembers + " has a bigger log index.";

        Collection<RaftEndpoint> endpoints = new HashSet<RaftEndpoint>(members());
        if (changeType == MembershipChangeType.ADD) {
            endpoints.add(endpoint);
        } else  {
            endpoints.remove(endpoint);
        }

        RaftGroupMembers newGroupMembers = new RaftGroupMembers(logIndex, endpoints, localEndpoint);
        committedGroupMembers = lastGroupMembers;
        lastGroupMembers = newGroupMembers;

        if (leaderState != null) {
            if (changeType == MembershipChangeType.ADD) {
                leaderState.add(endpoint, log.lastLogOrSnapshotIndex());
            } else {
                leaderState.remove(endpoint);
            }
        }
    }

    public void commitGroupMembers() {
        assert committedGroupMembers != lastGroupMembers
                : "Cannot commit last group members: " + lastGroupMembers + " because it is same with committed group members";

        committedGroupMembers = lastGroupMembers;
    }

    public void resetGroupMembers() {
        assert this.committedGroupMembers != this.lastGroupMembers;

        this.lastGroupMembers = this.committedGroupMembers;
    }

    public void restoreGroupMembers(int logIndex, Collection<RaftEndpoint> endpoints) {
        RaftGroupMembers groupMembers = new RaftGroupMembers(logIndex, endpoints, localEndpoint);
        this.committedGroupMembers = groupMembers;
        this.lastGroupMembers = groupMembers;
    }
}
