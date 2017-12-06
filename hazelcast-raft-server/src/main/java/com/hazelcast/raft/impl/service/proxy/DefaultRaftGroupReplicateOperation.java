package com.hazelcast.raft.impl.service.proxy;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.operation.RaftOperation;
import com.hazelcast.raft.impl.service.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;

import java.io.IOException;

public class DefaultRaftGroupReplicateOperation
        extends RaftReplicateOperation {

    private RaftGroupId groupId;

    private RaftOperation raftOperation;

    public DefaultRaftGroupReplicateOperation() {
    }

    public DefaultRaftGroupReplicateOperation(RaftGroupId groupId, RaftOperation raftOperation) {
        this.groupId = groupId;
        this.raftOperation = raftOperation;
    }

    @Override
    protected RaftGroupId getRaftGroupId() {
        return groupId;
    }

    @Override
    protected RaftOperation getRaftOperation() {
        return raftOperation;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.DEFAULT_RAFT_GROUP_REPLICATING_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(raftOperation);
        out.writeObject(groupId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        raftOperation = in.readObject();
        groupId = in.readObject();
    }
}
