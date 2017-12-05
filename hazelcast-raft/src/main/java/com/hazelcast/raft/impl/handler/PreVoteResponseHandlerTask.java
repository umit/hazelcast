package com.hazelcast.raft.impl.handler;

import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.dto.PreVoteResponse;
import com.hazelcast.raft.impl.state.CandidateState;
import com.hazelcast.raft.impl.state.RaftState;

/**
 * TODO: Javadoc Pending...
 *
 */
public class PreVoteResponseHandlerTask extends RaftNodeAwareTask implements Runnable {
    private final PreVoteResponse resp;

    public PreVoteResponseHandlerTask(RaftNode raftNode, PreVoteResponse response) {
        super(raftNode, true);
        this.resp = response;
    }

    @Override
    protected void innerRun() {
        RaftState state = raftNode.state();

        if (state.role() != RaftRole.FOLLOWER) {
            logger.info("Ignored " + resp + ". We are not FOLLOWER anymore.");
            return;
        }

        if (resp.term() < state.term()) {
            logger.warning("Stale " + resp + " is received, current term: " + state.term());
            return;
        }

        CandidateState preCandidateState = state.preCandidateState();
        if (preCandidateState == null) {
            logger.fine("Ignoring " + resp + ". We are not interested in pre-votes anymore.");
            return;
        }

        if (resp.granted() && preCandidateState.grantVote(resp.voter())) {
            logger.info("Pre-vote granted from " + resp.voter() + " for term: " + resp.term()
                    + ", number of votes: " + preCandidateState.voteCount() + ", majority: " + preCandidateState.majority());
        }

        if (preCandidateState.isMajorityGranted()) {
            logger.info("We have the majority during pre-vote phase. Let's start real election!");
            new LeaderElectionTask(raftNode).run();
        }
    }

    @Override
    protected RaftEndpoint senderEndpoint() {
        return resp.voter();
    }
}
