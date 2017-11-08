package com.hazelcast.raft.impl.handler;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.impl.RaftEndpoint;
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
        // TODO: Timeout should be configurable.
        int timeout = RandomPicker.getInt(1000, 3000);
        RaftState state = raftNode.state();

        if (state.leader() != null) {
            logger.severe("No new election round, we already have a LEADER: " + state.leader());
            return;
        }

        VoteRequest voteRequest = state.toCandidate();
        logger.warning("Leader election started at term: " + voteRequest.term());

        for (RaftEndpoint endpoint : state.remoteMembers()) {
            raftNode.send(new VoteRequestOp(state.name(), voteRequest), endpoint);
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
