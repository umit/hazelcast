package com.hazelcast.raft.impl.state;

import com.hazelcast.raft.impl.RaftEndpoint;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftGroupMembers {

    private final int index;

    private final Collection<RaftEndpoint> members;

    private final Collection<RaftEndpoint> remoteMembers;

    public RaftGroupMembers(int index, Collection<RaftEndpoint> endpoints, RaftEndpoint localEndpoint) {
        this.index = index;
        this.members = unmodifiableSet(new HashSet<RaftEndpoint>(endpoints));
        Set<RaftEndpoint> remoteMembers = new HashSet<RaftEndpoint>(endpoints);
        remoteMembers.remove(localEndpoint);
        this.remoteMembers = unmodifiableSet(remoteMembers);
    }

    public int index() {
        return index;
    }

    public Collection<RaftEndpoint> members() {
        return members;
    }

    public Collection<RaftEndpoint> remoteMembers() {
        return remoteMembers;
    }

    public int memberCount() {
        return members.size();
    }

    public int majority() {
        return members.size() / 2 + 1;
    }

    public boolean isKnownEndpoint(RaftEndpoint endpoint) {
        return members.contains(endpoint);
    }

    @Override
    public String toString() {
        return "RaftGroupMembers{" + "index=" + index + ", members=" + members + '}';
    }

}
