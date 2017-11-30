package com.hazelcast.raft.impl.util;

import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftNode.RaftNodeStatus;
import com.hazelcast.util.executor.StripedRunnable;

public class RaftNodeStatusAwareStripedRunnableAdaptor implements StripedRunnable {

    private final RaftNode raftNode;
    private final Runnable command;
    private final int key;

    public RaftNodeStatusAwareStripedRunnableAdaptor(RaftNode raftNode, Runnable command, int key) {
        this.raftNode = raftNode;
        this.command = command;
        this.key = key;
    }

    @Override
    public int getKey() {
        return key;
    }

    @Override
    public void run() {
        if (raftNode.getStatus() == RaftNodeStatus.TERMINATED) {
            return;
        }

        command.run();
    }

}
