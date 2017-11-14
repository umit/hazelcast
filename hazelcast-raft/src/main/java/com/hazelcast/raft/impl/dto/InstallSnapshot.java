package com.hazelcast.raft.impl.dto;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftDataSerializerHook;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.log.LogEntry;

import java.io.IOException;

public class InstallSnapshot implements IdentifiedDataSerializable {

    private RaftEndpoint leader;

    private LogEntry snapshot;

    public InstallSnapshot() {
    }

    public InstallSnapshot(RaftEndpoint leader, LogEntry snapshot) {
        this.leader = leader;
        this.snapshot = snapshot;
    }

    public RaftEndpoint leader() {
        return leader;
    }

    public LogEntry snapshot() {
        return snapshot;
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.INSTALL_SNAPSHOT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        leader.writeData(out);
        snapshot.writeData(out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        leader = new RaftEndpoint();
        leader.readData(in);
        snapshot = new LogEntry();
        snapshot.readData(in);
    }

    @Override
    public String toString() {
        return "InstallSnapshot{" + "leader=" + leader + ", snapshot=" + snapshot + '}';
    }

}
