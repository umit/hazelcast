package com.hazelcast.raft.service.atomiclong.proxy;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftOperation;
import com.hazelcast.raft.service.atomiclong.operation.GetAndSetOperation;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class GetAndSetReplicatingOperation extends AbstractAtomicLongReplicatingOperation {

    private long value;

    public GetAndSetReplicatingOperation() {
    }

    public GetAndSetReplicatingOperation(String name, long value) {
        super(name);
        this.value = value;
    }

    @Override
    protected RaftOperation getRaftOperation() {
        return new GetAndSetOperation(name, value);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(value);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        value = in.readLong();
    }
}
