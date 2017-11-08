package com.hazelcast.raft.impl.handler;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.impl.RaftEndpoint;
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
        if (!state.isKnownEndpoint(req.candidate())) {
            logger.warning("Ignoring " + req + " since candidate is unknown to us");
            return;
        }

        RaftEndpoint localEndpoint = raftNode.getLocalEndpoint();

        // Reply false if term < currentTerm (§5.1)
        if (state.term() > req.term()) {
            logger.warning("Rejecting " + req + " since current term: " + state.term() + " is greater than request term: "
                    + req.term());
            VoteResponse resp = new VoteResponse(localEndpoint, state.term(), false);
            raftNode.send(new VoteResponseOp(state.name(), resp), req.candidate());
            return;
        }

        if (state.term() < req.term()) {
            // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
            logger.warning("Demoting to FOLLOWER after " + req + " since current term: " + state.term()
                    + " is lower than request term: " + req.term());
            state.toFollower(req.term());
        }

        if (state.leader() != null && !req.candidate().equals(state.leader())) {
            logger.warning("Rejecting " + req + " since we have a leader " + state.leader());
            VoteResponse resp = new VoteResponse(localEndpoint, req.term(), false);
            raftNode.send(new VoteResponseOp(state.name(), resp), req.candidate());
            return;
        }

        if (state.lastVoteTerm() == req.term() && state.votedFor() != null) {
            logger.warning("Duplicate RequestVote for same term: " + req.term() + ", currently voted-for: " + state.votedFor());
            boolean granted = (req.candidate().equals(state.votedFor()));
            if (granted) {
                logger.warning("Duplicate " + req);
            }
            VoteResponse resp = new VoteResponse(localEndpoint, req.term(), granted);
            raftNode.send(new VoteResponseOp(state.name(), resp), req.candidate());
            return;
        }

        RaftLog raftLog = state.log();
        if (raftLog.lastLogTerm() > req.lastLogTerm()) {
            logger.warning("Rejecting " + req + " since our last log term: " + raftLog.lastLogTerm()
                    + " is greater than request last log term: " + req.lastLogTerm());
            VoteResponse resp = new VoteResponse(localEndpoint, req.term(), false);
            raftNode.send(new VoteResponseOp(state.name(), resp), req.candidate());
            return;
        }

        if (raftLog.lastLogTerm() == req.lastLogTerm() && raftLog.lastLogIndex() > req.lastLogIndex()) {
            logger.warning("Rejecting " + req + " since our last log index: " + raftLog.lastLogIndex() + " is greater");
            VoteResponse resp = new VoteResponse(localEndpoint, req.term(), false);
            raftNode.send(new VoteResponseOp(state.name(), resp), req.candidate());
            return;
        }

        logger.warning("Granted vote for " + req);
        state.persistVote(req.term(), req.candidate());

        VoteResponse resp = new VoteResponse(localEndpoint, req.term(), true);
        raftNode.send(new VoteResponseOp(state.name(), resp), req.candidate());
    }

    @Override
    public int getKey() {
        return raftNode.getStripeKey();
    }
}
