package com.hazelcast.raft.impl.handler;

import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.util.executor.StripedRunnable;

/**
 * TODO: Javadoc Pending...
 *
 */
public class PreVoteTimeoutTask implements StripedRunnable {
    private final RaftNode raftNode;

    public PreVoteTimeoutTask(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    @Override
    public void run() {
        if (raftNode.state().role() != RaftRole.FOLLOWER) {
            return;
        }
        raftNode.getLogger(getClass()).warning("Pre-vote for term: " + raftNode.state().term() + " has timed out!");
        new PreVoteTask(raftNode).run();
    }

    @Override
    public int getKey() {
        return raftNode.getStripeKey();
    }
}
