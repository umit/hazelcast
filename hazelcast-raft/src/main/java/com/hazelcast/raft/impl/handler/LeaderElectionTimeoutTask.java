package com.hazelcast.raft.impl.handler;

import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftRole;

/**
 * TODO: Javadoc Pending...
 *
 */
public class LeaderElectionTimeoutTask extends RaftNodeAwareTask implements Runnable {

    public LeaderElectionTimeoutTask(RaftNode raftNode) {
        super(raftNode, false);
    }

    @Override
    protected void innerRun() {
        if (raftNode.state().role() != RaftRole.CANDIDATE) {
            return;
        }
        logger.warning("Leader election for term: " + raftNode.state().term() + " has timed out!");
        new LeaderElectionTask(raftNode).run();
    }

    @Override
    protected RaftEndpoint senderEndpoint() {
        return null;
    }
}
