package com.hazelcast.raft.impl.handler;

import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftRole;

/**
 * TODO: Javadoc Pending...
 *
 */
public class PreVoteTimeoutTask extends RaftNodeAwareTask implements Runnable {

    public PreVoteTimeoutTask(RaftNode raftNode) {
        super(raftNode, false);
    }

    @Override
    protected void innerRun() {
        if (raftNode.state().role() != RaftRole.FOLLOWER) {
            return;
        }
        logger.warning("Pre-vote for term: " + raftNode.state().term() + " has timed out!");
        new PreVoteTask(raftNode).run();
    }

    @Override
    protected RaftEndpoint senderEndpoint() {
        return null;
    }
}
