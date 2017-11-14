package com.hazelcast.raft;

import com.hazelcast.raft.impl.RaftEndpoint;

/**
 * TODO: Javadoc Pending...
 */
public class NotLeaderException extends RaftException {

    public NotLeaderException(RaftEndpoint local, RaftEndpoint leader) {
        super(local.getAddress() + " is not LEADER. Known leader is: "
                + (leader != null ? leader.getAddress() : "N/A") , leader);
    }
}
