package com.hazelcast.raft.impl;

import com.hazelcast.raft.impl.RaftNode.RaftNodeStatus;

import static com.hazelcast.raft.impl.RaftNode.RaftNodeStatus.STEPPED_DOWN;
import static com.hazelcast.raft.impl.RaftNode.RaftNodeStatus.TERMINATED;
import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * TODO: Javadoc Pending...
 *
 */
public class ForceSetRaftNodeStatusRunnable implements Runnable {

    private final RaftNode raftNode;

    private final RaftNodeStatus status;

    public ForceSetRaftNodeStatusRunnable(RaftNode raftNode, RaftNodeStatus status) {
        checkTrue(status == TERMINATED || status == STEPPED_DOWN, "cannot force-set status to " + status);
        this.raftNode = raftNode;
        this.status = status;
    }

    @Override
    public void run() {
        if (!raftNode.isTerminatedOrSteppedDown()) {
            raftNode.setStatus(status);
        }
    }

}
