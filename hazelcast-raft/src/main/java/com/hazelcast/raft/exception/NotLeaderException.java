package com.hazelcast.raft.exception;

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftEndpoint;

/**
 * TODO: Javadoc Pending...
 */
public class NotLeaderException extends RaftException {
    public NotLeaderException(RaftGroupId groupId, RaftEndpoint local, RaftEndpoint leader) {
        super(local.getAddress() + " is not LEADER of " + groupId + ". Known leader is: "
                + (leader != null ? leader.getAddress() : "N/A") , leader);
    }
}
