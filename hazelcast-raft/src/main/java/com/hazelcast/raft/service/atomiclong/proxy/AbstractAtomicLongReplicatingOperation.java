package com.hazelcast.raft.service.atomiclong.proxy;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.impl.service.proxy.RaftReplicatingOperation;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLongService;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public abstract class AbstractAtomicLongReplicatingOperation extends RaftReplicatingOperation {

    protected String name;

    public AbstractAtomicLongReplicatingOperation() {
    }

    public AbstractAtomicLongReplicatingOperation(String name) {
        this.name = name;
    }

    @Override
    protected final String getRaftName() {
        return RaftAtomicLongService.PREFIX + name;
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
