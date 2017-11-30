package com.hazelcast.raft.impl.handler;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.raft.impl.dto.InstallSnapshot;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.state.RaftState;

public class InstallSnapshotHandlerTask implements Runnable {

    private final RaftNode raftNode;
    private final InstallSnapshot req;
    private final ILogger logger;

    public InstallSnapshotHandlerTask(RaftNode raftNode, InstallSnapshot req) {
        this.raftNode = raftNode;
        this.req = req;
        this.logger = raftNode.getLogger(getClass());
    }

    @Override
    public void run() {
        if (logger.isFineEnabled()) {
            logger.fine("Received " + req);
        }

        RaftState state = raftNode.state();
        if (!state.isKnownEndpoint(req.leader())) {
            logger.warning("Ignored " + req + ", since sender is unknown to us");
            return;
        }

        LogEntry snapshot = req.snapshot();

        // Reply false if term < currentTerm (§5.1)
        if (req.term() < state.term()) {
            logger.warning("Stale snapshot: " + req + " received in current term: " + state.term());
            AppendFailureResponse resp = new AppendFailureResponse(raftNode.getLocalEndpoint(), state.term(), snapshot.index() + 1);
            raftNode.send(resp, req.leader());
            return;
        }

        // Increase the term if we see a newer one, also transform to follower
        if (req.term() > state.term()) {
            // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
            logger.info("Demoting to FOLLOWER from current term: " + state.term() + " to new term: " + req.term()
                    + " and leader: " + req.leader());
            state.toFollower(req.term());
            raftNode.printMemberState();
        }

        if (!req.leader().equals(state.leader())) {
            logger.info("Setting leader: " + req.leader());
            state.leader(req.leader());
            raftNode.printMemberState();
        }

        if (raftNode.installSnapshot(snapshot)) {
            raftNode.send(new AppendSuccessResponse(raftNode.getLocalEndpoint(), req.term(), snapshot.index()), req.leader());
        }
    }

}
