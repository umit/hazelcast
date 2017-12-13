package com.hazelcast.raft.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.operation.RaftOperation;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.RaftDataSerializerHook;

import java.io.IOException;

public class RestoreSnapshotOp extends RaftOperation implements IdentifiedDataSerializable {

    private RaftGroupId groupId;
    private int commitIndex;
    private Object snapshot;

    public RestoreSnapshotOp() {
    }

    public RestoreSnapshotOp(String serviceName, RaftGroupId groupId, int commitIndex, Object snapshot) {
        this.groupId = groupId;
        setServiceName(serviceName);
        this.commitIndex = commitIndex;
        this.snapshot = snapshot;
    }

    @Override
    public Object doRun(int commitIndex) {
        assert this.commitIndex == commitIndex : " expected restore commit index: " + this.commitIndex + " given commit index: "
                + commitIndex;

        SnapshotAwareService service = getService();
        service.restoreSnapshot(groupId, commitIndex, snapshot);
        return null;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(groupId);
        out.writeInt(commitIndex);
        out.writeObject(snapshot);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        groupId = in.readObject();
        commitIndex = in.readInt();
        snapshot = in.readObject();
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
        return "RestoreSnapshotOp{" + "groupId='" + groupId + '\'' + ", commitIndex=" + commitIndex + ", snapshot=" + snapshot + '}';
    }
}
