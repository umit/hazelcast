package com.hazelcast.raft.impl.async;

import com.hazelcast.nio.Address;
import com.hazelcast.raft.impl.LogEntry;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.RaftState;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendResponse;
import com.hazelcast.raft.impl.operation.AppendEntriesOp;
import com.hazelcast.raft.impl.util.AddressableExecutionCallback;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.executor.StripedRunnable;

import static com.hazelcast.raft.impl.RaftService.SERVICE_NAME;

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

        AddressableExecutionCallback<AppendResponse> callback =
                new AppendEntriesExecutionCallback(raftNode, request, state.majority(), resultFuture);

        OperationService operationService = raftNode.getNodeEngine().getOperationService();
        for (Address address : state.members()) {
            if (thisAddress.equals(address)) {
                continue;
            }

            AppendEntriesOp op = new AppendEntriesOp(state.name(), request);
            InternalCompletableFuture<AppendResponse> future = operationService.invokeOnTarget(SERVICE_NAME, op, address);
            raftNode.registerCallback(future, address, callback);
        }
    }

    @Override
    public int getKey() {
        return raftNode.getStripeKey();
    }
}
