package com.hazelcast.raft.impl;

import com.hazelcast.nio.Address;

import java.util.Collection;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftState {

    private final Address thisAddress;
    private final String name;
    private final Collection<Address> members;

    private RaftRole role = RaftRole.FOLLOWER;
    private int term;
    private Address leader;

    // index of highest committed log entry
    private int commitIndex;

    // index of highest log entry that's applied to state
    // lastApplied <= commitIndex
    private int lastApplied;

    private Address votedFor;
    private int lastVoteTerm;

    private RaftLog log = new RaftLog();

    private LeaderState leaderState;
    private CandidateState candidateState;

    public RaftState(String name, Address thisAddress, Collection<Address> members) {
        this.name = name;
        this.thisAddress = thisAddress;
        this.members = members;
    }

    public String name() {
        return name;
    }

    public Collection<Address> members() {
        return members;
    }

    public RaftRole role() {
        return role;
    }

    public int term() {
        return term;
    }

    public int incTerm() {
        return ++term;
    }

    public Address leader() {
        return leader;
    }

    public int lastVoteTerm() {
        return lastVoteTerm;
    }

    public Address votedFor() {
        return votedFor;
    }

    public void leader(Address address) {
        leader = address;
    }

    public int commitIndex() {
        return commitIndex;
    }

    public void commitIndex(int idx) {
        commitIndex = idx;
    }

    public int lastApplied() {
        return lastApplied;
    }

    public void lastApplied(int index) {
        lastApplied = index;
    }

    public int majority() {
        return members.size() / 2 + 1;
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

    public void persistVote(int term, Address address) {
        this.lastVoteTerm = term;
        this.votedFor = address;
    }

    public void toFollower(int term) {
        role = RaftRole.FOLLOWER;
        leaderState = null;
        candidateState = null;
        this.term = term;
    }

    public void toCandidate() {
        role = RaftRole.CANDIDATE;
        leaderState = null;
        candidateState = new CandidateState(majority());
        candidateState.grantVote(thisAddress);
        persistVote(incTerm(), thisAddress);
    }

    public void toLeader() {
        role = RaftRole.LEADER;
        leader(thisAddress);
        candidateState = null;
        leaderState = new LeaderState(members, thisAddress, log.lastLogIndex());
    }
}
