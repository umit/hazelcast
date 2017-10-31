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

    final Map<Address, Integer> nextIndices = new HashMap<Address, Integer>();

    final Map<Address, Integer> matchIndices = new HashMap<Address, Integer>();

    public LeaderState(Collection<Address> addresses, int lastLogIndex) {
        for (Address address : addresses) {
            nextIndices.put(address, lastLogIndex + 1);
            matchIndices.put(address, 0);
        }
    }
}
