package com.hazelcast.raft.impl.handler;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.dto.PreVoteRequest;
import com.hazelcast.raft.impl.log.RaftLog;
import com.hazelcast.raft.impl.state.RaftState;
import com.hazelcast.util.executor.StripedRunnable;

import java.util.concurrent.TimeUnit;

/**
 * TODO: Javadoc Pending...
 *
 */
public class PreVoteTask implements StripedRunnable {
    private final RaftNode raftNode;
    private final ILogger logger;

    public PreVoteTask(RaftNode raftNode) {
        this.raftNode = raftNode;
        this.logger = raftNode.getLogger(getClass());
    }

    @Override
    public void run() {
        RaftState state = raftNode.state();

        if (state.leader() != null) {
            logger.warning("No new pre-vote phase, we already have a LEADER: " + state.leader());
            return;
        }

        int nextTerm = state.term() + 1;
        RaftLog log = state.log();
        PreVoteRequest voteRequest = new PreVoteRequest(raftNode.getLocalEndpoint(), nextTerm,
                log.lastLogOrSnapshotTerm(), log.lastLogOrSnapshotIndex());

        logger.info("Pre-vote started for next-term: " + voteRequest.nextTerm());
        raftNode.printMemberState();

        for (RaftEndpoint endpoint : state.remoteMembers()) {
            raftNode.send(voteRequest, endpoint);
        }

        schedulePreVoteTimeout();
    }

    private void schedulePreVoteTimeout() {
        raftNode.taskScheduler().schedule(new Runnable() {
            @Override
            public void run() {
                raftNode.execute(new PreVoteTimeoutTask(raftNode));
            }
        }, raftNode.getLeaderElectionTimeoutInMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int getKey() {
        return raftNode.getStripeKey();
    }

}
