package com.hazelcast.raft.impl.async;

import com.hazelcast.nio.Address;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftState;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.raft.impl.operation.VoteResponseOp;
import com.hazelcast.util.executor.StripedRunnable;

/**
 * TODO: Javadoc Pending...
 *
 */
public class VoteRequestTask implements StripedRunnable {
    private RaftNode raftNode;
    private final VoteRequest req;

    public VoteRequestTask(RaftNode raftNode, VoteRequest req) {
        this.raftNode = raftNode;
        this.req = req;
    }

    @Override
    public void run() {
        VoteResponse resp = new VoteResponse();
        resp.voter = raftNode.getNodeEngine().getThisAddress();
        try {
            RaftState state = raftNode.state();
            if (state.leader() != null && !req.candidate.equals(state.leader())) {
                raftNode.logger.warning("Rejecting vote request from " + req.candidate + " since we have a leader " + state.leader());
                rejectVoteResponse(resp);
                return;
            }
            if (state.term() > req.term) {
                raftNode.logger.warning(
                        "Rejecting vote request from " + req.candidate + " since our term is greater " + state.term() + " > " + req.term);
                rejectVoteResponse(resp);
                return;
            }

            if (state.term() < req.term) {
                raftNode.logger.warning("Demoting to FOLLOWER after vote request from " + req.candidate
                        + " since our term is lower " + state.term() + " < " + req.term);
                state.toFollower(req.term);
                resp.term = req.term;
            }

            if (state.lastVoteTerm() == req.term && state.votedFor() != null) {
                raftNode.logger.warning("Duplicate RequestVote for same term " + req.term + ", currently voted-for " + state.votedFor());
                if (req.candidate.equals(state.votedFor())) {
                    raftNode.logger.warning("Duplicate RequestVote from candidate " + req.candidate);
                    resp.granted = true;
                }
                return;
            }

            if (state.lastLogTerm() > req.lastLogTerm) {
                raftNode.logger.warning("Rejecting vote request from " + req.candidate + " since our last term is greater "
                        + state.lastLogTerm() + " > " + req.lastLogTerm);
                return;
            }

            if (state.lastLogTerm() == req.lastLogTerm && state.lastLogIndex() > req.lastLogIndex) {
                raftNode.logger.warning("Rejecting vote request from " + req.candidate + " since our last index is greater "
                        + state.lastLogIndex() + " > " + req.lastLogIndex);
                return;
            }

            raftNode.logger.warning("Granted vote for " + req.candidate + ", term: " + req.term);
            state.persistVote(req.term, req.candidate);
            resp.granted = true;

        } finally {
            sendResponse(resp, req.candidate);
        }
    }

    private void sendResponse(VoteResponse resp, Address candidate) {
        VoteResponseOp op = new VoteResponseOp(raftNode.state().name(), resp);
        raftNode.getNodeEngine().getOperationService().send(op, candidate);
    }

    private void rejectVoteResponse(VoteResponse response) {
        response.granted = false;
        response.term = raftNode.state().term();
    }

    @Override
    public int getKey() {
        return raftNode.getStripeKey();
    }
}
