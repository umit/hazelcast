package com.hazelcast.raft.impl.task;

import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.RaftRole;

/**
 * TODO: Javadoc Pending...
 *
 */
public class LeaderElectionTimeoutTask extends RaftNodeAwareTask implements Runnable {

    public LeaderElectionTimeoutTask(RaftNodeImpl raftNode) {
        super(raftNode);
    }

    @Override
    protected void innerRun() {
        if (raftNode.state().role() != RaftRole.CANDIDATE) {
            return;
        }
        logger.warning("Leader election for term: " + raftNode.state().term() + " has timed out!");
        new LeaderElectionTask(raftNode).run();
    }
}
