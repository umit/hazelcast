package com.hazelcast.raft.impl.operation;

import com.hazelcast.raft.RaftOperation;

/**
 * TODO: Javadoc Pending...
 *
 */
public abstract class InternalRaftOp extends RaftOperation {

    @Override
    protected final Object doRun(int commitIndex) {
        throw new UnsupportedOperationException();
    }

}
