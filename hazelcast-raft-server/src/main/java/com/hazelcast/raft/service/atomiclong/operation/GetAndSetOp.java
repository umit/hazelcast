package com.hazelcast.raft.service.atomiclong.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.atomiclong.AtomicLongDataSerializerHook;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLong;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 */
public class GetAndSetOp extends AbstractAtomicLongOp {

    private long value;

    public GetAndSetOp() {
    }

    public GetAndSetOp(String name, long value) {
        super(name);
        this.value = value;
    }

    @Override
    public Object doRun(RaftGroupId groupId, long commitIndex) {
        RaftAtomicLong atomic = getAtomicLong();
        return atomic.getAndSet(value);
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

    @Override
    public int getId() {
        return AtomicLongDataSerializerHook.GET_AND_SET_OP;
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", value=").append(value);
    }
}
