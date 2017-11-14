package com.hazelcast.raft;

import com.hazelcast.raft.impl.RaftEndpoint;

/**
 * TODO: Javadoc Pending...
 */
public class StaleAppendRequestException extends RaftException {

    public StaleAppendRequestException(RaftEndpoint leader) {
        super(leader);
    }
}
