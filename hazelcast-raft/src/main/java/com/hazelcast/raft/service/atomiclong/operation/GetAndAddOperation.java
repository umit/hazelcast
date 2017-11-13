package com.hazelcast.raft.service.atomiclong.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLong;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 */
public class GetAndAddOperation extends AbstractAtomicLongOperation {

    private long delta;

    public GetAndAddOperation() {
    }

    public GetAndAddOperation(String name, long delta) {
        super(name);
        this.delta = delta;
    }

    @Override
    public Object doRun(int commitIndex) {
        RaftAtomicLong atomic = getAtomicLong();
        return atomic.getAndAdd(delta, commitIndex);
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
