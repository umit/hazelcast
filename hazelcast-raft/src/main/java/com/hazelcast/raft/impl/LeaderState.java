package com.hazelcast.raft.impl;

import com.hazelcast.nio.Address;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * TODO: Javadoc Pending...
 *
 */
public class LeaderState {

    private final Map<Address, Integer> nextIndices = new HashMap<Address, Integer>();

    private final Map<Address, Integer> matchIndices = new HashMap<Address, Integer>();

    public LeaderState(Collection<Address> remoteMembers, int lastLogIndex) {
        for (Address follower : remoteMembers) {
            nextIndices.put(follower, lastLogIndex + 1);
            matchIndices.put(follower, 0);
        }
    }

    public void setNextIndex(Address follower, int index) {
        assertFollower(nextIndices, follower);
        assert index > 0 : "Invalid next index: " + index;
        nextIndices.put(follower, index);
    }

    public void setMatchIndex(Address follower, int index) {
        assertFollower(matchIndices, follower);
        assert index >= 0 : "Invalid match index: " + index;
        matchIndices.put(follower, index);
    }

    public Collection<Integer> matchIndices() {
        return matchIndices.values();
    }

    public int getNextIndex(Address follower) {
        assertFollower(nextIndices, follower);
        return nextIndices.get(follower);
    }

    public int getMatchIndex(Address follower) {
        assertFollower(matchIndices, follower);
        return matchIndices.get(follower);
    }

    private void assertFollower(Map<Address, Integer> indices, Address follower) {
        assert indices.containsKey(follower) : "Unknown address " + follower;
    }

}
