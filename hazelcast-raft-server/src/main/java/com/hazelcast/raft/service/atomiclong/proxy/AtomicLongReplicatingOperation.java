package com.hazelcast.raft.service.atomiclong.proxy;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftOperation;
import com.hazelcast.raft.impl.service.RaftGroupId;
import com.hazelcast.raft.impl.service.proxy.RaftReplicatingOperation;
import com.hazelcast.raft.service.atomiclong.AtomicLongDataSerializerHook;
import com.hazelcast.raft.service.atomiclong.operation.AbstractAtomicLongOperation;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public final class AtomicLongReplicatingOperation extends RaftReplicatingOperation {

    private AbstractAtomicLongOperation operation;

    public AtomicLongReplicatingOperation() {
    }

    public AtomicLongReplicatingOperation(AbstractAtomicLongOperation operation) {
        this.operation = operation;
    }

    @Override
    protected RaftOperation getRaftOperation() {
        return operation;
    }

    @Override
    protected RaftGroupId getRaftGroupId() {
        return operation.getRaftGroupId();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(operation);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        operation = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return AtomicLongDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return AtomicLongDataSerializerHook.REPLICATING_OP;
    }
}
