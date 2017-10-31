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


    public RaftState(String name, Address thisAddress, Collection<Address> members) {
        this.name = name;
        this.thisAddress = thisAddress;
        this.members = members;
    }

    public String name() {
        return name;
    }

    public void role(RaftRole role) {
        this.role = role;
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

    public void persistVote(int term, Address address) {
        this.lastVoteTerm = term;
        this.votedFor = address;
    }

    public void term(int term) {
        this.term = term;
    }

    public int lastVoteTerm() {
        return lastVoteTerm;
    }

    public Address votedFor() {
        return votedFor;
    }

    public int lastLogTerm() {
        return log.lastLogTerm();
    }

    public int lastLogIndex() {
        return log.lastLogIndex();
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

    public void selfLeader() {
        role(RaftRole.LEADER);
        leader(thisAddress);
        leaderState = new LeaderState(members, log.lastLogIndex());
    }

    public LogEntry getLog(int index) {
        return log.getLog(index);
    }

    public void deleteLogRange(int from, int to) {
        log.deleteRange(from, to);
    }

    public void deleteLogAfter(int index) {
        log.deleteAfter(index);
    }

    public void storeLogs(LogEntry... newEntries) {
        log.store(newEntries);
    }

    public LeaderState getLeaderState() {
        return leaderState;
    }
}
