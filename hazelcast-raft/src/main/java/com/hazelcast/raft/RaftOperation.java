package com.hazelcast.raft;

import com.hazelcast.spi.Operation;

/**
 * TODO: Javadoc Pending...
 *
 */
public abstract class RaftOperation extends Operation {

    private int commitIndex;

    private Object response;

    public abstract Object doRun(int commitIndex);

    // TODO basri I think this one should not be public
    public final RaftOperation setCommitIndex(int commitIndex) {
        this.commitIndex = commitIndex;
        return this;
    }

    @Override
    public final void run() throws Exception {
        response = doRun(commitIndex);
    }

    @Override
    public final boolean returnsResponse() {
        return true;
    }

    @Override
    public final Object getResponse() {
        return response;
    }

}
