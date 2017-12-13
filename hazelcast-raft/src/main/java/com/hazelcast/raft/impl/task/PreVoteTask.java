package com.hazelcast.raft.impl.task;

import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.dto.PreVoteRequest;
import com.hazelcast.raft.impl.log.RaftLog;
import com.hazelcast.raft.impl.state.RaftState;

/**
 * TODO: Javadoc Pending...
 *
 */
public class PreVoteTask extends RaftNodeAwareTask implements Runnable {

    public PreVoteTask(RaftNode raftNode) {
        super(raftNode);
    }

    @Override
    protected void innerRun() {
        RaftState state = raftNode.state();

        if (state.leader() != null) {
            logger.warning("No new pre-vote phase, we already have a LEADER: " + state.leader());
            return;
        }

        state.initPreCandidateState();
        int nextTerm = state.term() + 1;
        RaftLog log = state.log();
        PreVoteRequest request = new PreVoteRequest(raftNode.getLocalEndpoint(), nextTerm,
                log.lastLogOrSnapshotTerm(), log.lastLogOrSnapshotIndex());

        logger.info("Pre-vote started for next term: " + request.nextTerm() + ", last log index: " + request.lastLogIndex()
                + ", last log term: " + request.lastLogTerm());
        raftNode.printMemberState();

        for (RaftEndpoint endpoint : state.remoteMembers()) {
            raftNode.send(request, endpoint);
        }

        schedulePreVoteTimeout();
    }

    private void schedulePreVoteTimeout() {
        raftNode.schedule(new PreVoteTimeoutTask(raftNode), raftNode.getLeaderElectionTimeoutInMillis());
    }
}
