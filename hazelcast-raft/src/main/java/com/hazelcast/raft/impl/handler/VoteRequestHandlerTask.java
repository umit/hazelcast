package com.hazelcast.raft.impl.handler;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.impl.RaftLog;
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
public class VoteRequestHandlerTask implements StripedRunnable {
    private final ILogger logger;
    private RaftNode raftNode;
    private final VoteRequest req;

    public VoteRequestHandlerTask(RaftNode raftNode, VoteRequest req) {
        this.raftNode = raftNode;
        this.req = req;
        this.logger = raftNode.getLogger(getClass());
    }

    @Override
    public void run() {
        RaftState state = raftNode.state();
        if (!state.isKnownEndpoint(req.candidate)) {
            logger.warning("Ignoring " + req + " since candidate is unknown to us");
            return;
        }

        VoteResponse resp = new VoteResponse();
        resp.voter = raftNode.getLocalEndpoint();
        try {

            if (state.leader() != null && !req.candidate.equals(state.leader())) {
                logger.warning("Rejecting vote request from " + req.candidate + " since we have a leader " + state.leader());
                rejectVoteResponse(resp);
                return;
            }
            // Reply false if term < currentTerm (§5.1)
            if (state.term() > req.term) {
                logger.warning("Rejecting vote request from " + req.candidate + " since current term: " + state.term()
                        + " is greater than request term: " + req.term);
                rejectVoteResponse(resp);
                return;
            }

            // If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log,
            // grant vote (§5.2, §5.4)

            if (state.term() < req.term) {
                // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
                logger.warning("Demoting to FOLLOWER after vote request from " + req.candidate
                        + " since current term: " + state.term() + " is lower than request term: " + req.term);
                state.toFollower(req.term);
                resp.term = req.term;
            }

            if (state.lastVoteTerm() == req.term && state.votedFor() != null) {
                logger.warning("Duplicate RequestVote for same term: " + req.term + ", currently voted-for: " + state.votedFor());
                if (req.candidate.equals(state.votedFor())) {
                    logger.warning("Duplicate RequestVote from " + req.candidate);
                    resp.granted = true;
                }
                return;
            }

            RaftLog raftLog = state.log();
            if (raftLog.lastLogTerm() > req.lastLogTerm) {
                logger.warning("Rejecting vote request from " + req.candidate + " since our last log term: "
                        + raftLog.lastLogTerm() + " is greater than request last log term: " + req.lastLogTerm);
                return;
            }

            if (raftLog.lastLogTerm() == req.lastLogTerm && raftLog.lastLogIndex() > req.lastLogIndex) {
                logger.warning("Rejecting vote request from " + req.candidate + " since our last log index: "
                        + raftLog.lastLogIndex() + " is greater than request last log index: " + req.lastLogIndex);
                return;
            }

            logger.warning("Granted vote for " + req.candidate + ", term: " + req.term);
            state.persistVote(req.term, req.candidate);
            resp.granted = true;

        } finally {
            raftNode.send(new VoteResponseOp(raftNode.state().name(), resp), req.candidate);
        }
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
