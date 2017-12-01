package com.hazelcast.raft.impl.handler;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNode;

/**
 * TODO: Javadoc Pending...
 *
 */
public abstract class RaftNodeAwareTask implements Runnable {

    protected final RaftNode raftNode;
    protected final ILogger logger;

    protected RaftNodeAwareTask(RaftNode raftNode) {
        this.raftNode = raftNode;
        this.logger = raftNode.getLogger(getClass());
    }

    @Override
    public final void run() {
        if (raftNode.isTerminated()) {
            logger.fine("Won't run, since raft node is terminated");
            return;
        }

        RaftEndpoint sender = senderEndpoint();
        if (sender != null && !raftNode.state().isKnownEndpoint(sender)) {
            logger.warning("Won't run, since " + sender + " is unknown to us");
            return;
        }

        innerRun();
    }

    protected abstract void innerRun();

    protected abstract RaftEndpoint senderEndpoint();

}
