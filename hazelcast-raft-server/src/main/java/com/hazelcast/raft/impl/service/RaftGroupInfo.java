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
    private RaftEndpoint leavingMember;
    private RaftEndpoint joiningMember;

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

    public RaftEndpoint joiningMember() {
        return joiningMember;
    }

    public RaftEndpoint leavingMember() {
        return leavingMember;
    }

    public void markSubstitutes(RaftEndpoint leaving, RaftEndpoint joining) {
        if (leavingMember != null) {
            if (!leavingMember.equals(leaving)) {
                throw new IllegalStateException(leavingMember + " is already leaving. "
                        + "Cannot substitute " + leaving + " with " + joining);
            }
            if (!joiningMember.equals(joining)) {
                throw new IllegalStateException(joining + " is already joining. "
                        + "Cannot substitute " + leaving + " with " + joining);
            }
            return;
        }
        if (!members.contains(leaving)) {
            throw new IllegalArgumentException(leaving + " doesn't exist in the group!");
        }
        if (members.contains(joining)) {
            throw new IllegalArgumentException(joining + " is already in the group!");
        }
        leavingMember = leaving;
        joiningMember = joining;
    }

    public void completeSubstitution(RaftEndpoint endpoint, int newMembersCommitIndex) {
        if (leavingMember == null || !leavingMember.equals(endpoint)) {
            throw new IllegalArgumentException("Cannot remove " + endpoint + ", current leaving member is " + leavingMember);
        }
        Set<RaftEndpoint> set = new HashSet<RaftEndpoint>(members);
        set.remove(endpoint);
        set.add(joiningMember);
        members = Collections.unmodifiableSet(set);
        membersCommitIndex = newMembersCommitIndex;
        membersArray = members.toArray(new RaftEndpoint[0]);
        leavingMember = null;
        joiningMember = null;
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
        out.writeObject(leavingMember);
        out.writeObject(joiningMember);
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
        leavingMember = in.readObject();
        joiningMember = in.readObject();
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
        return "RaftGroupInfo{" + "id=" + id + ", members=" + members + ", serviceName='" + serviceName + '\''
                + ", leavingMember=" + leavingMember + ", joiningMember=" + joiningMember + ", status=" + status + '}';
    }
}
