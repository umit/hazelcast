package com.hazelcast.raft.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftOperation;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.RaftDataSerializerHook;
import com.hazelcast.raft.impl.RaftEndpoint;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

public class RestoreSnapshotOp extends RaftOperation implements IdentifiedDataSerializable {

    private String name;
    private int commitIndex;
    private Object snapshot;
    private int groupMembersLogIndex;
    private Collection<RaftEndpoint> groupMembers;

    public RestoreSnapshotOp() {
    }

    public RestoreSnapshotOp(String serviceName, String name, int commitIndex, Object snapshot,
                             int groupMembersLogIndex, Collection<RaftEndpoint> groupMembers) {
        this.name = name;
        setServiceName(serviceName);
        this.commitIndex = commitIndex;
        this.snapshot = snapshot;
        this.groupMembersLogIndex = groupMembersLogIndex;
        this.groupMembers = groupMembers;
    }

    public int getGroupMembersLogIndex() {
        return groupMembersLogIndex;
    }

    public Collection<RaftEndpoint> getGroupMembers() {
        return groupMembers;
    }

    @Override
    public Object doRun(int commitIndex) {
        assert this.commitIndex == commitIndex : " expected restore commit index: " + this.commitIndex + " given commit index: "
                + commitIndex;

        SnapshotAwareService service = getService();
        service.restoreSnapshot(name, commitIndex, snapshot);
        return null;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(name);
        out.writeInt(commitIndex);
        out.writeObject(snapshot);
        out.writeInt(groupMembersLogIndex);
        out.writeInt(groupMembers.size());
        for (RaftEndpoint endpoint : groupMembers) {
            endpoint.writeData(out);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readUTF();
        commitIndex = in.readInt();
        snapshot = in.readObject();
        groupMembersLogIndex = in.readInt();
        int count = in.readInt();
        groupMembers = new HashSet<RaftEndpoint>(count);
        for (int i = 0; i < count; i++) {
            RaftEndpoint endpoint = new RaftEndpoint();
            endpoint.readData(in);
            groupMembers.add(endpoint);
        }
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.RESTORE_SNAPSHOT_OP;
    }

    @Override
    public String toString() {
        return "RestoreSnapshotOp{" + "name='" + name + '\'' + ", commitIndex=" + commitIndex + ", snapshot=" + snapshot
                + ", groupMembersLogIndex=" + groupMembersLogIndex + ", groupMembers=" + groupMembers + '}';
    }
}
