package com.hazelcast.raft.impl.handler;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.state.RaftState;
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
        RaftState state = raftNode.state();

        if (state.leader() != null) {
            logger.warning("No new election round, we already have a LEADER: " + state.leader());
            return;
        }

        VoteRequest voteRequest = state.toCandidate();
        logger.info("Leader election started for term: " + voteRequest.term());
        raftNode.printMemberState();

        for (RaftEndpoint endpoint : state.remoteMembers()) {
            raftNode.send(voteRequest, endpoint);
        }

        scheduleLeaderElectionTimeout();
    }

    private void scheduleLeaderElectionTimeout() {
        raftNode.taskScheduler().schedule(new Runnable() {
            @Override
            public void run() {
                raftNode.execute(new LeaderElectionTimeoutTask(raftNode));
            }
        }, raftNode.getLeaderElectionTimeoutInMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int getKey() {
        return raftNode.getStripeKey();
    }

}
