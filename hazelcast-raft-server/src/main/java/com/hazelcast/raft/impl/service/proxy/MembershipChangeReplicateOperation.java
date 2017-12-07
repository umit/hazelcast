package com.hazelcast.raft.impl.service.proxy;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.MembershipChangeType;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.service.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.operation.RaftOperation;

import java.io.IOException;

public class MembershipChangeReplicateOperation extends RaftReplicateOperation {

    private RaftEndpoint endpoint;
    private MembershipChangeType changeType;

    public MembershipChangeReplicateOperation() {
    }

    public MembershipChangeReplicateOperation(RaftGroupId groupId, RaftEndpoint endpoint,
            MembershipChangeType changeType) {
        super(groupId);
        this.endpoint = endpoint;
        this.changeType = changeType;
    }

    @Override
    ICompletableFuture replicate(RaftNode raftNode) {
        return raftNode.replicateMembershipChange(endpoint, changeType);
    }

    @Override
    protected RaftOperation getRaftOperation() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.MEMBERSHIP_CHANGE_REPLICATE_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(endpoint);
        out.writeUTF(changeType.toString());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        endpoint = in.readObject();
        changeType = MembershipChangeType.valueOf(in.readUTF());
    }
}
