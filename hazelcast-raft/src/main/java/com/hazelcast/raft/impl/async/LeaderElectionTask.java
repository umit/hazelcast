package com.hazelcast.raft.impl.async;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftState;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.operation.VoteRequestOp;
import com.hazelcast.util.RandomPicker;
import com.hazelcast.util.executor.StripedRunnable;

import java.util.concurrent.TimeUnit;

/**
 * TODO: Javadoc Pending...
 *
 */
public class LeaderElectionTask implements StripedRunnable {
    private final RaftNode raftNode;
    private final ILogger logger;

    public LeaderElectionTask(RaftNode raftNode) {
        this.raftNode = raftNode;
        this.logger = raftNode.getLogger(getClass());
    }

    @Override
    public void run() {
        int timeout = RandomPicker.getInt(1000, 3000);
        RaftState state = raftNode.state();

        if (state.leader() != null) {
            logger.severe("No election, we have a leader: " + state.leader());
            return;
        }

        logger.warning("Leader election start");
        VoteRequest voteRequest = state.toCandidate();

        if (state.memberCount() == 1) {
            state.toLeader();
            logger.warning("We are the one! ");
            return;
        }

        for (Address address : state.remoteMembers()) {
            raftNode.send(new VoteRequestOp(state.name(), voteRequest), address);
        }

        scheduleLeaderElectionTimeout(timeout);
    }

    private void scheduleLeaderElectionTimeout(long timeout) {
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
