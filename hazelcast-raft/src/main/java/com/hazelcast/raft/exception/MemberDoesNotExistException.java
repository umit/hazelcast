package com.hazelcast.raft.exception;

import com.hazelcast.raft.impl.RaftEndpoint;

/**
 * TODO: Javadoc Pending...
 */
public class MemberDoesNotExistException extends RaftException {

    public MemberDoesNotExistException(RaftEndpoint member) {
        super("Member does not exist: " + member, null);
    }
}
