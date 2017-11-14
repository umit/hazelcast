package com.hazelcast.raft;

import com.hazelcast.raft.impl.RaftEndpoint;

/**
 * TODO: Javadoc Pending...
 */
public class LeaderDemotedException extends RaftException {

    public LeaderDemotedException(RaftEndpoint local, RaftEndpoint leader) {
        super(local.getAddress() + " is not LEADER anymore. Known leader is: "
                + (leader != null ? leader.getAddress() : "N/A") , leader);
    }
}
