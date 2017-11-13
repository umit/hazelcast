package com.hazelcast.raft.service.atomiclong.proxy;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftOperation;
import com.hazelcast.raft.service.atomiclong.operation.CompareAndSetOperation;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class CompareAndSetReplicatingOperation extends AbstractAtomicLongReplicatingOperation {

    private long currentValue;
    private long newValue;

    public CompareAndSetReplicatingOperation() {
    }

    public CompareAndSetReplicatingOperation(String name, long currentValue, long newValue) {
        super(name);
        this.currentValue = currentValue;
        this.newValue = newValue;
    }

    @Override
    protected RaftOperation getRaftOperation() {
        return new CompareAndSetOperation(name, currentValue, newValue);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(currentValue);
        out.writeLong(newValue);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        currentValue = in.readLong();
        newValue = in.readLong();
    }
}
