package com.hazelcast.raft.impl.handler;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.state.RaftState;

/**
 * TODO: Javadoc Pending...
 *
 */
public class LeaderElectionTask implements Runnable {
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

        VoteRequest request = state.toCandidate();
        logger.info("Leader election started for term: " + request.term() + ", last log index: " + request.lastLogIndex()
                + ", last log term: " + request.lastLogTerm());
        raftNode.printMemberState();

        for (RaftEndpoint endpoint : state.remoteMembers()) {
            raftNode.send(request, endpoint);
        }

        scheduleLeaderElectionTimeout();
    }

    private void scheduleLeaderElectionTimeout() {
        raftNode.schedule(new LeaderElectionTimeoutTask(raftNode), raftNode.getLeaderElectionTimeoutInMillis());
    }

}
