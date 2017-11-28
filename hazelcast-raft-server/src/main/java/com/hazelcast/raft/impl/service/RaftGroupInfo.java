package com.hazelcast.raft.impl.service;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftEndpoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * TODO: Javadoc Pending...
 *
 */
public final class RaftGroupInfo implements IdentifiedDataSerializable {

    private String serviceName;
    private String name;
    private Collection<RaftEndpoint> members;
    private int commitIndex;

    private transient RaftEndpoint[] membersArray;

    public RaftGroupInfo(String serviceName, String name, Collection<RaftEndpoint> members, int commitIndex) {
        this.serviceName = serviceName;
        this.name = name;
        this.members = Collections.unmodifiableCollection(members);
        this.commitIndex = commitIndex;
    }

    public RaftGroupInfo() {
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
        return members.size();
    }

    public int commitIndex() {
        return commitIndex;
    }

    public RaftEndpoint[] membersArray() {
        if (membersArray == null) {
            membersArray = members.toArray(new RaftEndpoint[0]);
        }
        return membersArray;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(serviceName);
        out.writeUTF(name);
        out.writeInt(commitIndex);
        out.writeInt(members.size());
        for (RaftEndpoint endpoint : members) {
            out.writeObject(endpoint);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        serviceName = in.readUTF();
        name = in.readUTF();
        commitIndex = in.readInt();
        int len = in.readInt();
        members = new ArrayList<RaftEndpoint>(len);
        for (int i = 0; i < len; i++) {
            RaftEndpoint endpoint = in.readObject();
            members.add(endpoint);
        }
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
