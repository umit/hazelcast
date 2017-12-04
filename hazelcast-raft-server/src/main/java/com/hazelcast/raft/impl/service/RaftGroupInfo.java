package com.hazelcast.raft.impl.service;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftEndpoint;

import java.io.IOException;
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
public final class RaftGroupInfo implements IdentifiedDataSerializable {

    public enum RaftGroupStatus {
        ACTIVE, DESTROYING, DESTROYED
    }

    private RaftGroupId id;
    private Collection<RaftEndpoint> members;
    private String serviceName;

    private volatile RaftGroupStatus status;

    private transient RaftEndpoint[] membersArray;

    public RaftGroupInfo() {
    }

    public RaftGroupInfo(RaftGroupId id, Collection<RaftEndpoint> members, String serviceName) {
        this.id = id;
        this.members = unmodifiableCollection(new ArrayList<RaftEndpoint>(members));
        this.serviceName = serviceName;
        this.status = ACTIVE;
        this.membersArray = members.toArray(new RaftEndpoint[0]);
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
        members = new ArrayList<RaftEndpoint>(len);
        for (int i = 0; i < len; i++) {
            RaftEndpoint endpoint = in.readObject();
            members.add(endpoint);
        }
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
}
