package com.hazelcast.raft.impl.service;

import com.hazelcast.raft.impl.RaftEndpoint;

import java.util.ArrayList;
import java.util.Collection;

import static com.hazelcast.raft.impl.service.RaftGroupInfo.RaftGroupStatus.ACTIVE;
import static com.hazelcast.raft.impl.service.RaftGroupInfo.RaftGroupStatus.DESTROYED;
import static com.hazelcast.raft.impl.service.RaftGroupInfo.RaftGroupStatus.DESTROYING;
import static com.hazelcast.util.Preconditions.checkState;
import static java.util.Collections.unmodifiableCollection;

/**
 * TODO: Javadoc Pending...
 *
 */
public final class RaftGroupInfo {

    public enum RaftGroupStatus {
        ACTIVE, DESTROYING, DESTROYED
    }

    private RaftGroupId id;
    private Collection<RaftEndpoint> members;
    private String serviceName;
    // read from multiple threads in tests
    private volatile RaftGroupStatus status;

    private transient RaftEndpoint[] membersArray;

    public RaftGroupInfo(RaftGroupId id, Collection<RaftEndpoint> members, String serviceName) {
        this.id = id;
        this.members = unmodifiableCollection(new ArrayList<RaftEndpoint>(members));
        this.serviceName = serviceName;
        this.status = ACTIVE;
    }

    public RaftGroupId id() {
        return id;
    }

    public String name() {
        return id.name();
    }

    public int commitIndex() {
        return id.commitIndex();
    }

    public Collection<RaftEndpoint> members() {
        return members;
    }

    public int memberCount() {
        return members.size();
    }

    public String serviceName() {
        return serviceName;
    }

    public RaftGroupStatus status() {
        return status;
    }

    public boolean setDestroying() {
        if (status == DESTROYED) {
            return false;
        }

        status = DESTROYING;
        return true;
    }

    public boolean setDestroyed() {
        checkState(status != ACTIVE, "Cannot destroy " + id + " because status is: " + status);

        if (status == DESTROYED) {
            return false;
        }

        status = DESTROYED;
        return true;
    }

    public RaftEndpoint[] membersArray() {
        if (membersArray == null) {
            membersArray = members.toArray(new RaftEndpoint[0]);
        }
        return membersArray;
    }
}
