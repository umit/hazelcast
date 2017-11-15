package com.hazelcast.raft.exception;

import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.spi.exception.RetryableException;

/**
 * TODO: Javadoc Pending...
 */
public class UncommittedEntryLimitExceededException extends RaftException implements RetryableException {

    public UncommittedEntryLimitExceededException(RaftEndpoint leader) {
        super("There are too many inflight appended entries to be committed", leader);
    }
}
