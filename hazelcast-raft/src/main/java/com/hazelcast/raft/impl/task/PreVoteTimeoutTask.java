package com.hazelcast.raft.impl.task;

import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.RaftRole;

/**
 * TODO: Javadoc Pending...
 *
 */
public class PreVoteTimeoutTask extends RaftNodeAwareTask implements Runnable {

    public PreVoteTimeoutTask(RaftNodeImpl raftNode) {
        super(raftNode);
    }

    @Override
    protected void innerRun() {
        if (raftNode.state().role() != RaftRole.FOLLOWER) {
            return;
        }
        logger.info("Pre-vote for term: " + raftNode.state().term() + " has timed out!");
        new PreVoteTask(raftNode).run();
    }
}
