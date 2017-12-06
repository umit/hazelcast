package com.hazelcast.raft.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.operation.RaftOperation;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.RaftDataSerializerHook;

import java.io.IOException;

public class RestoreSnapshotOp extends RaftOperation implements IdentifiedDataSerializable {

    private String name;
    private int commitIndex;
    private Object snapshot;

    public RestoreSnapshotOp() {
    }

    public RestoreSnapshotOp(String serviceName, String name, int commitIndex, Object snapshot) {
        this.name = name;
        setServiceName(serviceName);
        this.commitIndex = commitIndex;
        this.snapshot = snapshot;
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
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readUTF();
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
        return "RestoreSnapshotOp{" + "name='" + name + '\'' + ", commitIndex=" + commitIndex + ", snapshot=" + snapshot + '}';
    }
}
