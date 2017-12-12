package com.hazelcast.raft.impl.service;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftEndpoint;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.raft.impl.service.RaftGroupInfo.RaftGroupStatus.ACTIVE;
import static com.hazelcast.raft.impl.service.RaftGroupInfo.RaftGroupStatus.DESTROYED;
import static com.hazelcast.raft.impl.service.RaftGroupInfo.RaftGroupStatus.DESTROYING;
import static com.hazelcast.util.Preconditions.checkState;

/**
 * TODO: Javadoc Pending...
 *
 */
public final class RaftGroupInfo implements IdentifiedDataSerializable {

    public enum RaftGroupStatus {
        ACTIVE, DESTROYING, DESTROYED
    }

    private RaftGroupId id;
    private int membersCommitIndex;
    private Set<RaftEndpoint> members;
    private String serviceName;

    // read outside of Raft
    private volatile RaftGroupStatus status;

    private transient RaftEndpoint[] membersArray;

    public RaftGroupInfo() {
    }

    public RaftGroupInfo(RaftGroupId id, Collection<RaftEndpoint> endpoints, String serviceName) {
        this.id = id;
        this.serviceName = serviceName;
        this.status = ACTIVE;
        this.members = Collections.unmodifiableSet(new HashSet<RaftEndpoint>(endpoints));
        this.membersArray = endpoints.toArray(new RaftEndpoint[0]);
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

    public boolean containsMember(RaftEndpoint endpoint) {
        return members.contains(endpoint);
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

    public int getMembersCommitIndex() {
        return membersCommitIndex;
    }

    public boolean substitute(RaftEndpoint leaving, RaftEndpoint joining,
            int expectedMembersCommitIndex, int newMembersCommitIndex) {
        if (membersCommitIndex != expectedMembersCommitIndex) {
            return false;
        }

        Set<RaftEndpoint> set = new HashSet<RaftEndpoint>(members);
        boolean removed = set.remove(leaving);
        assert removed : leaving + " is not member of " + toString();
        boolean added = set.add(joining);
        assert added : joining + " is already member of " + toString();

        members = Collections.unmodifiableSet(set);
        membersCommitIndex = newMembersCommitIndex;
        membersArray = members.toArray(new RaftEndpoint[0]);
        return true;
    }

    public RaftEndpoint[] membersArray() {
        return membersArray;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(id);
        out.writeInt(members.size());
        for (RaftEndpoint endpoint : members) {
            out.writeObject(endpoint);
        }
        out.writeUTF(serviceName);
        out.writeUTF(status.toString());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        id = in.readObject();
        int len = in.readInt();
        members = new HashSet<RaftEndpoint>(len);
        for (int i = 0; i < len; i++) {
            RaftEndpoint endpoint = in.readObject();
            members.add(endpoint);
        }
        members = Collections.unmodifiableSet(members);
        serviceName = in.readUTF();
        status = RaftGroupStatus.valueOf(in.readUTF());
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.GROUP_INFO;
    }

    @Override
    public String toString() {
        return "RaftGroupInfo{" + "id=" + id + ", membersCommitIndex=" + membersCommitIndex + ", members=" + members
                + ", serviceName='" + serviceName + '\'' + ", status=" + status + '}';
    }
}
