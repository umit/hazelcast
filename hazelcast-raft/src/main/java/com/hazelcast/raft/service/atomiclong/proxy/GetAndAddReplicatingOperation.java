package com.hazelcast.raft.service.atomiclong.proxy;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftOperation;
import com.hazelcast.raft.service.atomiclong.operation.GetAndAddOperation;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class GetAndAddReplicatingOperation extends AbstractAtomicLongReplicatingOperation {

    private long delta;

    public GetAndAddReplicatingOperation() {
    }

    public GetAndAddReplicatingOperation(String name, long delta) {
        super(name);
        this.delta = delta;
    }

    @Override
    protected RaftOperation getRaftOperation() {
        return new GetAndAddOperation(name, delta);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(delta);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        delta = in.readLong();
    }
}
