package com.hazelcast.raft.impl.async;

import com.hazelcast.nio.Address;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftState;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.operation.VoteRequestOp;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.RandomPicker;
import com.hazelcast.util.executor.StripedRunnable;

import java.util.concurrent.TimeUnit;

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
        state.toCandidate();

        if (state.members().size() == 1) {
            state.toLeader();
            raftNode.logger.warning("We are the one! ");
            return;
        }

        OperationService operationService = raftNode.getNodeEngine().getOperationService();

        for (Address address : state.members()) {
            if (address.equals(thisAddress)) {
                continue;
            }

            VoteRequest request =
                    new VoteRequest(state.term(), thisAddress, state.lastLogTerm(), state.lastLogIndex());
            VoteRequestOp op = new VoteRequestOp(state.name(), request);

            operationService.send(op, address);
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
