package com.hazelcast.raft;

import java.util.Collection;
import java.util.HashSet;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftConfig {

    private final Collection<RaftMember> members = new HashSet<RaftMember>();

    public Collection<RaftMember> getMembers() {
        return members;
    }

    public RaftConfig setMembers(Collection<RaftMember> members) {
        this.members.clear();
        this.members.addAll(members);
        return this;
    }

    public RaftConfig addMember(RaftMember member) {
        members.add(member);
        return this;
    }

}
