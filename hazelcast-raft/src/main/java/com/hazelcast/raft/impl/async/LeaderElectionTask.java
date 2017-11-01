package com.hazelcast.raft.impl.async;

import com.hazelcast.nio.Address;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.RaftState;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.raft.impl.operation.RequestVoteOp;
import com.hazelcast.raft.impl.util.AddressableExecutionCallback;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.RandomPicker;
import com.hazelcast.util.executor.StripedRunnable;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.impl.RaftService.SERVICE_NAME;

/**
 * TODO: Javadoc Pending...
 *
 */
public class LeaderElectionTask implements StripedRunnable {
    private final RaftNode raftNode;

    public LeaderElectionTask(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    @Override
    public void run() {
        Address thisAddress = raftNode.getNodeEngine().getThisAddress();
        int timeout = RandomPicker.getInt(1000, 3000);
        RaftState state = raftNode.state();
        if (state.votedFor() != null && !thisAddress.equals(state.votedFor())) {
            scheduleTimeout(timeout);
            return;
        }

        raftNode.logger.warning("Leader election start");
        state.role(RaftRole.CANDIDATE);
        state.persistVote(state.incTerm(), thisAddress);

        if (state.members().size() == 1) {
            state.role(RaftRole.LEADER);
            raftNode.logger.warning("We are the one! ");
            return;
        }


        OperationService operationService = raftNode.getNodeEngine().getOperationService();
        AddressableExecutionCallback<VoteResponse>
                callback = new LeaderElectionExecutionCallback(raftNode, state.majority());
        for (Address address : state.members()) {
            if (address.equals(thisAddress)) {
                continue;
            }

            RequestVoteOp op = new RequestVoteOp(state.name(),
                    new VoteRequest(
                            state.term(), thisAddress, state.lastLogTerm(), state.lastLogIndex()));
            InternalCompletableFuture<VoteResponse> future =
                    operationService.createInvocationBuilder(SERVICE_NAME, op, address).setCallTimeout(timeout).invoke();
            raftNode.registerCallback(future, address, callback);
        }

        scheduleTimeout(timeout);
    }

    private void scheduleTimeout(int timeout) {
        raftNode.taskScheduler().schedule(new Runnable() {
            @Override
            public void run() {
                raftNode.executor().execute(new LeaderElectionTimeoutTask(raftNode));
            }
        }, timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public int getKey() {
        return raftNode.getStripeKey();
    }

}
