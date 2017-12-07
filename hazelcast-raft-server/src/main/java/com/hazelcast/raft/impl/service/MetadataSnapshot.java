package com.hazelcast.raft.impl.service;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftEndpoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * TODO: Javadoc Pending...
 *
 */
public final class MetadataSnapshot implements IdentifiedDataSerializable {

    private final Collection<RaftEndpoint> endpoints = new ArrayList<RaftEndpoint>();
    private final Collection<RaftEndpoint> shutdownEndpoints = new ArrayList<RaftEndpoint>();
    private final Collection<RaftGroupInfo> raftGroups = new ArrayList<RaftGroupInfo>();
    private RaftEndpoint shuttingDownEndpoint;

    public void addRaftGroup(RaftGroupInfo groupInfo) {
        raftGroups.add(groupInfo);
    }

    public void addEndpoint(RaftEndpoint endpoint) {
        endpoints.add(endpoint);
    }

    public void addShutdownEndpoint(RaftEndpoint endpoint) {
        shutdownEndpoints.add(endpoint);
    }

    public void setShuttingDownEndpoint(RaftEndpoint shuttingDownEndpoint) {
        this.shuttingDownEndpoint = shuttingDownEndpoint;
    }

    public Collection<RaftEndpoint> getEndpoints() {
        return endpoints;
    }

    public Collection<RaftEndpoint> getShutdownEndpoints() {
        return shutdownEndpoints;
    }

    public RaftEndpoint getShuttingDownEndpoint() {
        return shuttingDownEndpoint;
    }

    public Collection<RaftGroupInfo> getRaftGroups() {
        return raftGroups;
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
        out.writeInt(endpoints.size());
        for (RaftEndpoint endpoint : endpoints) {
            out.writeObject(endpoint);
        }
        out.writeInt(raftGroups.size());
        for (RaftGroupInfo group : raftGroups) {
            out.writeObject(group);
        }
        out.writeInt(shutdownEndpoints.size());
        for (RaftEndpoint endpoint : shutdownEndpoints) {
            out.writeObject(endpoint);
        }
        out.writeObject(shuttingDownEndpoint);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int len = in.readInt();
        for (int i = 0; i < len; i++) {
            RaftEndpoint endpoint = in.readObject();
            endpoints.add(endpoint);
        }

        len = in.readInt();
        for (int i = 0; i < len; i++) {
            RaftGroupInfo groupInfo = in.readObject();
            raftGroups.add(groupInfo);
        }

        len = in.readInt();
        for (int i = 0; i < len; i++) {
            RaftEndpoint endpoint = in.readObject();
            shutdownEndpoints.add(endpoint);
        }
        shuttingDownEndpoint = in.readObject();
    }
}
