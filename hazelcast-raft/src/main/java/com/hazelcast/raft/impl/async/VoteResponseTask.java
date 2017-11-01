package com.hazelcast.raft.impl.async;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.impl.CandidateState;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.RaftState;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.util.executor.StripedRunnable;

/**
 * TODO: Javadoc Pending...
 *
 */
public class VoteResponseTask implements StripedRunnable {
    private final RaftNode raftNode;
    private final VoteResponse resp;
    private final ILogger logger;

    public VoteResponseTask(RaftNode raftNode, VoteResponse response) {
        this.raftNode = raftNode;
        this.resp = response;
        this.logger = raftNode.getLogger(getClass());
    }

    @Override
    public void run() {
        RaftState state = raftNode.state();
        if (RaftRole.CANDIDATE != state.role()) {
            return;
        }

        if (resp.term > state.term()) {
            logger.warning("Newer term discovered, fallback to follower");
            state.toFollower(resp.term);
            return;
        }

        if (resp.term < state.term()) {
            logger.warning("Obsolete vote response received: " + resp + ", current-term: " + state.term());
            return;
        }

        CandidateState candidateState = state.candidateState();
        if (resp.granted && candidateState.grantVote(resp.voter)) {
                logger.warning("Vote granted from " + resp.voter + " for term " + state.term()
                                + ", number of votes: " + candidateState.voteCount()
                                + ", majority: " + candidateState.majority());
        }

        if (candidateState.isMajorityGranted()) {
            logger.severe("We are THE leader!");
            state.toLeader();
            raftNode.scheduleLeaderLoop();
        }
    }

    @Override
    public int getKey() {
        return raftNode.getStripeKey();
    }
}
