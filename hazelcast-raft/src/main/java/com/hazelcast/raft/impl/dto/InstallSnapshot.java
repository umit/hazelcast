package com.hazelcast.raft.impl.dto;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftDataSerializerHook;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.log.SnapshotEntry;

import java.io.IOException;

public class InstallSnapshot implements IdentifiedDataSerializable {

    private RaftEndpoint leader;

    private int term;

    private SnapshotEntry snapshot;

    public InstallSnapshot() {
    }

    public InstallSnapshot(RaftEndpoint leader, int term, SnapshotEntry snapshot) {
        this.leader = leader;
        this.term = term;
        this.snapshot = snapshot;
    }

    public RaftEndpoint leader() {
        return leader;
    }

    public int term() {
        return term;
    }

    public SnapshotEntry snapshot() {
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
        out.writeObject(leader);
        out.writeInt(term);
        out.writeObject(snapshot);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        leader = in.readObject();
        term = in.readInt();
        snapshot = in.readObject();
    }

    @Override
    public String toString() {
        return "InstallSnapshot{" + "leader=" + leader + ", term=" + term + ", snapshot=" + snapshot + '}';
    }

}
