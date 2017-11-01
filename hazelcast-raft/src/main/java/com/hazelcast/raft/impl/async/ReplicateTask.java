package com.hazelcast.raft.impl.async;

import com.hazelcast.nio.Address;
import com.hazelcast.raft.impl.LogEntry;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.RaftState;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.operation.AppendRequestOp;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.executor.StripedRunnable;

/**
 * TODO: Javadoc Pending...
 *
 */
public class ReplicateTask implements StripedRunnable {
    private final RaftNode raftNode;
    private final Object value;
    private final SimpleCompletableFuture resultFuture;

    public ReplicateTask(RaftNode raftNode, Object value, SimpleCompletableFuture resultFuture) {
        this.raftNode = raftNode;
        this.value = value;
        this.resultFuture = resultFuture;
    }

    @Override
    public void run() {

        // TODO: debug
        RaftState state = raftNode.state();
        if (state.role() != RaftRole.LEADER) {
            return;
        }
        raftNode.logger.info("Replicating: " + value);

        assert state.role() == RaftRole.LEADER;

        int lastLogIndex = state.lastLogIndex();
        int lastLogTerm = state.lastLogTerm();

        LogEntry entry = new LogEntry(state.term(), lastLogIndex + 1, value);
        state.storeLogs(entry);

        Address thisAddress = raftNode.getNodeEngine().getThisAddress();
        AppendRequest request =
                new AppendRequest(state.term(), thisAddress, lastLogTerm, lastLogIndex,
                        state.commitIndex(), new LogEntry[] {entry});

        OperationService operationService = raftNode.getNodeEngine().getOperationService();
        for (Address address : state.members()) {
            if (thisAddress.equals(address)) {
                continue;
            }

            AppendRequestOp op = new AppendRequestOp(state.name(), request);
            operationService.send(op, address);
        }
    }

    @Override
    public int getKey() {
        return raftNode.getStripeKey();
    }
}
