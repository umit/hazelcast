package com.hazelcast.raft.impl.state;

import com.hazelcast.raft.impl.RaftEndpoint;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Mutable state maintained by the leader of the Raft group. Leader keeps two indices for each server:
 * <ul>
 * <li>{@code nextIndex}: index of the next log entry to send to that server
 * (initialized to leader's {@code lastLogIndex + 1})</li>
 * <li>{@code matchIndex}: index of highest log entry known to be replicated on server
 * (initialized to 0, increases monotonically)</li>
 * </ul>
 */
public class LeaderState {

    private final Map<RaftEndpoint, Integer> nextIndices = new HashMap<RaftEndpoint, Integer>();

    private final Map<RaftEndpoint, Integer> matchIndices = new HashMap<RaftEndpoint, Integer>();

    public LeaderState(Collection<RaftEndpoint> remoteMembers, int lastLogIndex) {
        for (RaftEndpoint follower : remoteMembers) {
            nextIndices.put(follower, lastLogIndex + 1);
            matchIndices.put(follower, 0);
        }
    }

    /**
     * Add a new follower with the leader's {@code lastLogIndex}. Follower's {@code nextIndex} will be set
     * to {@code lastLogIndex + 1} and {@code matchIndex} to 0.
     */
    public void add(RaftEndpoint follower, int lastLogIndex) {
        assertNotFollower(nextIndices, follower);
        nextIndices.put(follower, lastLogIndex + 1);
        matchIndices.put(follower, 0);
    }

    /**
     * Removes a follower from leader maintained state.
     */
    public void remove(RaftEndpoint follower) {
        assertFollower(nextIndices, follower);
        nextIndices.remove(follower);
        matchIndices.remove(follower);
    }

    /**
     * Sets {@code nextIndex} for a known follower.
     */
    public void setNextIndex(RaftEndpoint follower, int index) {
        assertFollower(nextIndices, follower);
        assert index > 0 : "Invalid next index: " + index;
        nextIndices.put(follower, index);
    }

    /**
     * Sets {@code matchIndex} for a known follower.
     */
    public void setMatchIndex(RaftEndpoint follower, int index) {
        assertFollower(matchIndices, follower);
        assert index >= 0 : "Invalid match index: " + index;
        matchIndices.put(follower, index);
    }

    public Collection<Integer> matchIndices() {
        return matchIndices.values();
    }

    /**
     * Returns the {@code nextIndex} for a known follower.
     */
    public int getNextIndex(RaftEndpoint follower) {
        assertFollower(nextIndices, follower);
        return nextIndices.get(follower);
    }

    /**
     * Returns the {@code matchIndex} for a known follower.
     */
    public int getMatchIndex(RaftEndpoint follower) {
        assertFollower(matchIndices, follower);
        return matchIndices.get(follower);
    }

    private void assertFollower(Map<RaftEndpoint, Integer> indices, RaftEndpoint follower) {
        assert indices.containsKey(follower) : "Unknown follower " + follower;
    }

    private void assertNotFollower(Map<RaftEndpoint, Integer> indices, RaftEndpoint follower) {
        assert !indices.containsKey(follower) : "Already known follower " + follower;
    }

}
