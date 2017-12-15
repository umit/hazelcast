package com.hazelcast.raft.impl.task;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.impl.RaftNodeImpl;

/**
 * TODO: Javadoc Pending...
 *
 */
public abstract class RaftNodeAwareTask implements Runnable {

    protected final RaftNodeImpl raftNode;
    protected final ILogger logger;

    protected RaftNodeAwareTask(RaftNodeImpl raftNode) {
        this.raftNode = raftNode;
        this.logger = raftNode.getLogger(getClass());
    }

    @Override
    public final void run() {
        if (raftNode.isTerminatedOrSteppedDown()) {
            logger.fine("Won't run, since raft node is terminated");
            return;
        }

        innerRun();
    }

    protected abstract void innerRun();

}
