package com.hazelcast.raft;

import com.hazelcast.nio.Address;

import java.util.Collection;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftContext {

    final String name;
    final Collection<Address> members;

    private RaftRole role = RaftRole.FOLLOWER;
    private int term;
    private Address leader;

    private Address votedFor;
    private int lastVoteTerm;

    public RaftContext(String name, Collection<Address> members) {
        this.name = name;
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
        return 0;
    }

    public int lastLogIndex() {
        return 0;
    }

    public void leader(Address address) {
        leader = address;
    }
}
