package com.hazelcast.raft.impl.operation;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftDataSerializerHook;
import com.hazelcast.raft.operation.RaftOperation;

/**
 * TODO: Javadoc Pending...
 *
 */
public class NopEntryOp extends RaftOperation implements IdentifiedDataSerializable {

    @Override
    protected Object doRun(int commitIndex) {
        return null;
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.NOP_ENTRY_OP;
    }
}
