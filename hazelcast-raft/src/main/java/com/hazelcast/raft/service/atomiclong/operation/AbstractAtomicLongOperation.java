package com.hazelcast.raft.service.atomiclong.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftOperation;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLong;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLongService;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 */
public abstract class AbstractAtomicLongOperation extends RaftOperation {

    private String name;

    public AbstractAtomicLongOperation() {
    }

    public AbstractAtomicLongOperation(String name) {
        this.name = name;
    }

    protected RaftAtomicLong getAtomicLong() {
        RaftAtomicLongService service = getService();
        return service.getAtomicLong(name);
    }

    @Override
    public String getServiceName() {
        return RaftAtomicLongService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(name);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readUTF();
    }
}
