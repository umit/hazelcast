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

    public RaftGroupInfo(String serviceName, String name, Collection<RaftEndpoint> members) {
        this.serviceName = serviceName;
        this.name = name;
        this.members = Collections.unmodifiableCollection(members);
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getName() {
        return name;
    }

    public Collection<RaftEndpoint> getMembers() {
        return members;
    }
}
