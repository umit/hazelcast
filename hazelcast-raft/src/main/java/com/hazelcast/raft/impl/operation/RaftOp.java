package com.hazelcast.raft.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftDataSerializerHook;
import com.hazelcast.raft.impl.RaftService;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public abstract class RaftOp extends Operation implements IdentifiedDataSerializable {

    protected String name;

    public RaftOp() {
    }

    public RaftOp(String name) {
        this.name = name;
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
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

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }
}
