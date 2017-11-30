package com.hazelcast.raft.impl.handler;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.RaftOperation;
import com.hazelcast.raft.exception.CannotAppendException;
import com.hazelcast.raft.exception.NotLeaderException;
import com.hazelcast.raft.exception.RaftGroupTerminatedException;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftNode.RaftNodeStatus;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.operation.TerminateRaftGroupOp;
import com.hazelcast.raft.impl.state.RaftState;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;

/**
 * TODO: Javadoc Pending...
 *
 */
public class ReplicateTask implements Runnable {
    private final RaftNode raftNode;
    private final RaftOperation operation;
    private final SimpleCompletableFuture resultFuture;
    private final ILogger logger;

    public ReplicateTask(RaftNode raftNode, RaftOperation operation, SimpleCompletableFuture resultFuture) {
        this.raftNode = raftNode;
        this.operation = operation;
        this.logger = raftNode.getLogger(getClass());
        this.resultFuture = resultFuture;
    }

    @Override
    public void run() {
        if (!(operation instanceof TerminateRaftGroupOp || raftNode.getServiceName().equals(operation.getServiceName()))) {
            resultFuture.setResult(new IllegalArgumentException("operation: " + operation + "  service name: "
                    + operation.getServiceName() + " is different than expected service name: " + raftNode.getServiceName()));
            return;
        }

        if (raftNode.getStatus() == RaftNodeStatus.TERMINATED) {
            resultFuture.setResult(new RaftGroupTerminatedException());
            return;
        }

        RaftState state = raftNode.state();
        if (state.role() != RaftRole.LEADER) {
            resultFuture.setResult(new NotLeaderException(raftNode.getLocalEndpoint(), state.leader()));
            return;
        }

        if (!raftNode.shouldAllowNewAppends()) {
            resultFuture.setResult(new CannotAppendException(raftNode.getLocalEndpoint()));
            return;
        }

        if (logger.isFineEnabled()) {
            logger.fine("Replicating: " + operation + " in term: " + state.term());
        }

        int newEntryLogIndex = state.log().lastLogOrSnapshotIndex() + 1;
        raftNode.registerFuture(newEntryLogIndex, resultFuture);
        state.log().appendEntries(new LogEntry(state.term(), newEntryLogIndex, operation));

        if (operation instanceof TerminateRaftGroupOp) {
            raftNode.setStatus(RaftNodeStatus.TERMINATING);
        }

        raftNode.broadcastAppendRequest();
    }
}
