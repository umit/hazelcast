package com.hazelcast.raft.impl.task;

import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.state.RaftState;

/**
 * TODO: Javadoc Pending...
 *
 */
public class LeaderElectionTask extends RaftNodeAwareTask implements Runnable {

    public LeaderElectionTask(RaftNode raftNode) {
        super(raftNode);
    }

    @Override
    protected void innerRun() {
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
