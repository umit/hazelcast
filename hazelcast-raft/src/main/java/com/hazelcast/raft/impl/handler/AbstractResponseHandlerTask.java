package com.hazelcast.raft.impl.handler;

import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.task.RaftNodeAwareTask;

/**
 * TODO: Javadoc Pending...
 *
 */
public abstract class AbstractResponseHandlerTask extends RaftNodeAwareTask {

    public AbstractResponseHandlerTask(RaftNode raftNode) {
        super(raftNode);
    }

    @Override
    protected final void innerRun() {
        RaftEndpoint sender = senderEndpoint();
        if (!raftNode.state().isKnownEndpoint(sender)) {
            logger.warning("Won't run, since " + sender + " is unknown to us");
            return;
        }

        handleResponse();
    }

    protected abstract void handleResponse();

    protected abstract RaftEndpoint senderEndpoint();

}
