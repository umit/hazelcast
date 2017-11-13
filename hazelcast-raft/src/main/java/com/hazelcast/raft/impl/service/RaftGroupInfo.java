package com.hazelcast.raft.impl.service;

import com.hazelcast.raft.impl.RaftEndpoint;

import java.util.Collection;
import java.util.Collections;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftGroupInfo {

    private final String serviceName;
    private final String name;
    private final Collection<RaftEndpoint> members;
    private final RaftEndpoint[] membersArray;

    public RaftGroupInfo(String serviceName, String name, Collection<RaftEndpoint> members) {
        this.serviceName = serviceName;
        this.name = name;
        this.members = Collections.unmodifiableCollection(members);
        this.membersArray = members.toArray(new RaftEndpoint[0]);
    }

    public String serviceName() {
        return serviceName;
    }

    public String name() {
        return name;
    }

    public Collection<RaftEndpoint> members() {
        return members;
    }

    public int memberCount() {
        return membersArray.length;
    }

    public RaftEndpoint member(int index) {
        return membersArray[index];
    }
}
