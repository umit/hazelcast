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

    public LeaderState(Collection<Address> addresses, Address thisAddress, int lastLogIndex) {
        for (Address address : addresses) {
            if (address.equals(thisAddress)) {
                continue;
            }
            nextIndices.put(address, lastLogIndex + 1);
            matchIndices.put(address, 0);
        }
    }

    public void nextIndex(Address follower, int index) {
        assert nextIndices.containsKey(follower) : "Unknown address " + follower;
        nextIndices.put(follower, index);
    }

    public void matchIndex(Address follower, int index) {
        assert matchIndices.containsKey(follower) : "Unknown address " + follower;
        matchIndices.put(follower, index);
    }

    public Collection<Integer> matchIndices() {
        return matchIndices.values();
    }
}
