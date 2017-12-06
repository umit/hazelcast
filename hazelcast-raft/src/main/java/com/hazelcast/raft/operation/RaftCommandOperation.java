package com.hazelcast.raft.operation;

/**
 * TODO: Javadoc Pending...
 *
 */
public abstract class RaftCommandOperation extends RaftOperation {

    @Override
    protected final Object doRun(int commitIndex) {
        throw new UnsupportedOperationException();
    }

}
