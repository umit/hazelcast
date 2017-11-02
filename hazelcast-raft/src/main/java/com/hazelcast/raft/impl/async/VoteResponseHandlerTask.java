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
public class VoteResponseHandlerTask implements StripedRunnable {
    private final RaftNode raftNode;
    private final VoteResponse resp;
    private final ILogger logger;

    public VoteResponseHandlerTask(RaftNode raftNode, VoteResponse response) {
        this.raftNode = raftNode;
        this.resp = response;
        this.logger = raftNode.getLogger(getClass());
    }

    @Override
    public void run() {
        RaftState state = raftNode.state();
        if (state.role() != RaftRole.CANDIDATE) {
            logger.severe("Ignored " + resp + ". We are not CANDIDATE anymore.");
            return;
        }

        if (resp.term > state.term()) {
            logger.warning("Newer term: " + resp.term + " than current term: " + state.term()
                    + " is discovered, fallback to FOLLOWER");
            state.toFollower(resp.term);
            return;
        }

        if (resp.term < state.term()) {
            logger.warning("Stale " + resp + " is received, current term: " + state.term());
            return;
        }

        CandidateState candidateState = state.candidateState();
        if (resp.granted && candidateState.grantVote(resp.voter)) {
            logger.warning("Vote granted from " + resp.voter + " for term: " + state.term()
                    + ", number of votes: " + candidateState.voteCount() + ", majority: " + candidateState.majority());
        }

        if (candidateState.isMajorityGranted()) {
            logger.severe("We are the LEADER!");
            state.toLeader();
            raftNode.scheduleLeaderLoop();
        }
    }

    @Override
    public int getKey() {
        return raftNode.getStripeKey();
    }
}
