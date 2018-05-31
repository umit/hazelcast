package com.hazelcast.raft.impl.service;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftMember;
import com.hazelcast.raft.impl.RaftMemberImpl;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

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
    private long membersCommitIndex;
    // endpoint -> TRUE: initial-member | FALSE: substitute-member
    private Map<RaftMemberImpl, Boolean> members;

    // read outside of Raft
    private volatile RaftGroupStatus status;

    private transient RaftMemberImpl[] membersArray;

    public RaftGroupInfo() {
    }

    public RaftGroupInfo(RaftGroupId id, Collection<RaftMemberImpl> endpoints) {
        this.id = id;
        this.status = ACTIVE;
        LinkedHashMap<RaftMemberImpl, Boolean> map = new LinkedHashMap<RaftMemberImpl, Boolean>(endpoints.size());
        for (RaftMemberImpl endpoint : endpoints) {
            map.put(endpoint, Boolean.TRUE);
        }
        this.members = Collections.unmodifiableMap(map);
        this.membersArray = endpoints.toArray(new RaftMemberImpl[0]);
    }

    public RaftGroupId id() {
        return id;
    }

    public String name() {
        return id.name();
    }

    public long commitIndex() {
        return id.commitIndex();
    }

    @SuppressWarnings("unchecked")
    public Collection<RaftMember> members() {
        return (Collection) members.keySet();
    }

    public Collection<RaftMemberImpl> endpointImpls() {
        return members.keySet();
    }

    public boolean containsMember(RaftMemberImpl endpoint) {
        return members.containsKey(endpoint);
    }

    public int memberCount() {
        return members.size();
    }

    public boolean isInitialMember(RaftMemberImpl endpoint) {
        assert members.containsKey(endpoint);
        return members.get(endpoint);
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

    public long getMembersCommitIndex() {
        return membersCommitIndex;
    }

    public boolean substitute(RaftMemberImpl leaving, RaftMemberImpl joining,
                              long expectedMembersCommitIndex, long newMembersCommitIndex) {
        if (membersCommitIndex != expectedMembersCommitIndex) {
            return false;
        }

        Map<RaftMemberImpl, Boolean> map = new LinkedHashMap<RaftMemberImpl, Boolean>(members);
        Object removed = map.remove(leaving);
        assert removed != null : leaving + " is not member of " + toString();
        if (joining != null) {
            Object added = map.put(joining, Boolean.FALSE);
            assert added == null : joining + " is already member of " + toString();
        }

        members = Collections.unmodifiableMap(map);
        membersCommitIndex = newMembersCommitIndex;
        membersArray = members.keySet().toArray(new RaftMemberImpl[0]);
        return true;
    }

    public RaftMemberImpl[] membersArray() {
        return membersArray;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(id);
        out.writeLong(membersCommitIndex);
        out.writeInt(members.size());
        for (Map.Entry<RaftMemberImpl, Boolean> entry : members.entrySet()) {
            out.writeObject(entry.getKey());
            out.writeBoolean(entry.getValue());
        }
        out.writeUTF(status.toString());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        id = in.readObject();
        membersCommitIndex = in.readLong();
        int len = in.readInt();
        members = new LinkedHashMap<RaftMemberImpl, Boolean>(len);
        for (int i = 0; i < len; i++) {
            RaftMemberImpl endpoint = in.readObject();
            members.put(endpoint, in.readBoolean());
        }
        membersArray = members.keySet().toArray(new RaftMemberImpl[0]);
        members = Collections.unmodifiableMap(members);
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
        return "RaftGroupInfo{" + "id=" + id + ", membersCommitIndex=" + membersCommitIndex + ", members=" + members()
                + ", status=" + status + '}';
    }
}
