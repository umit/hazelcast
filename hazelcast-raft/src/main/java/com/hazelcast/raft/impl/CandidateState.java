package com.hazelcast.raft.impl;

import com.hazelcast.nio.Address;

import java.util.HashSet;
import java.util.Set;

/**
 * TODO: Javadoc Pending...
 *
 */
public class CandidateState {

    private final int majority;
    private final Set<Address> voters = new HashSet<Address>();

    public CandidateState(int majority) {
        this.majority = majority;
    }

    public boolean grantVote(Address address) {
        return voters.add(address);
    }

    public int majority() {
        return majority;
    }

    public int voteCount() {
        return voters.size();
    }

    public boolean isMajorityGranted() {
        return voteCount() >= majority();
    }
}
