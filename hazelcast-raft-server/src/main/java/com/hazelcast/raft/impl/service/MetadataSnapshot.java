package com.hazelcast.raft.impl.service;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftMemberImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * TODO: Javadoc Pending...
 *
 */
public final class MetadataSnapshot implements IdentifiedDataSerializable {

    private final Collection<RaftMemberImpl> members = new ArrayList<RaftMemberImpl>();
    private final Collection<RaftGroupInfo> raftGroups = new ArrayList<RaftGroupInfo>();
    private LeavingRaftEndpointContext leavingRaftEndpointContext;

    public void addRaftGroup(RaftGroupInfo group) {
        raftGroups.add(group);
    }

    public void addEndpoint(RaftMemberImpl endpoint) {
        members.add(endpoint);
    }

    public Collection<RaftMemberImpl> getMembers() {
        return members;
    }

    public Collection<RaftGroupInfo> getRaftGroups() {
        return raftGroups;
    }

    public LeavingRaftEndpointContext getLeavingRaftEndpointContext() {
        return leavingRaftEndpointContext;
    }

    public void setLeavingRaftEndpointContext(LeavingRaftEndpointContext leavingRaftEndpointContext) {
        this.leavingRaftEndpointContext = leavingRaftEndpointContext;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.METADATA_SNAPSHOT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(members.size());
        for (RaftMemberImpl endpoint : members) {
            out.writeObject(endpoint);
        }
        out.writeInt(raftGroups.size());
        for (RaftGroupInfo group : raftGroups) {
            out.writeObject(group);
        }
        out.writeObject(leavingRaftEndpointContext);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int len = in.readInt();
        for (int i = 0; i < len; i++) {
            RaftMemberImpl endpoint = in.readObject();
            members.add(endpoint);
        }

        len = in.readInt();
        for (int i = 0; i < len; i++) {
            RaftGroupInfo groupInfo = in.readObject();
            raftGroups.add(groupInfo);
        }
        leavingRaftEndpointContext = in.readObject();
    }
}
