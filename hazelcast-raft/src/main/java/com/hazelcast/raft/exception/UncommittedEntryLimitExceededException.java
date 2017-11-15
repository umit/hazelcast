package com.hazelcast.raft.exception;

import com.hazelcast.raft.impl.RaftEndpoint;

/**
 * TODO: Javadoc Pending...
 */
public class UncommittedEntryLimitExceededException extends RaftException {

    public UncommittedEntryLimitExceededException(RaftEndpoint leader) {
        super("There are too many inflight appended entries to be committed", leader);
    }
}
