package com.hazelcast.raft.impl.async;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.impl.LogEntry;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.RaftState;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.util.executor.StripedRunnable;

/**
 * TODO: Javadoc Pending...
 *
 */
public class ReplicateTask implements StripedRunnable {
    private final RaftNode raftNode;
    private final Object value;
    private final SimpleCompletableFuture resultFuture;
    private final ILogger logger;

    public ReplicateTask(RaftNode raftNode, Object value, SimpleCompletableFuture resultFuture) {
        this.raftNode = raftNode;
        this.value = value;
        this.logger = raftNode.getLogger(getClass());
        this.resultFuture = resultFuture;
    }

    @Override
    public void run() {
        // TODO: debug
        RaftState state = raftNode.state();
        if (state.role() != RaftRole.LEADER) {
            resultFuture.setResult(new IllegalStateException("We are not the LEADER!"));
            return;
        }

        logger.info("Replicating: " + value);

        assert state.role() == RaftRole.LEADER;

        int logIndex = state.log().lastLogIndex() + 1;
        raftNode.registerFuture(logIndex, resultFuture);
        state.log().appendEntries(new LogEntry(state.term(), logIndex, value));
        raftNode.broadcastAppendRequest();
    }

    @Override
    public int getKey() {
        return raftNode.getStripeKey();
    }
}
