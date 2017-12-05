package com.hazelcast.raft.impl.handler;

import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.raft.impl.dto.InstallSnapshot;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.state.RaftState;

public class InstallSnapshotHandlerTask extends RaftNodeAwareTask implements Runnable {

    private final InstallSnapshot req;

    public InstallSnapshotHandlerTask(RaftNode raftNode, InstallSnapshot req) {
        super(raftNode, false);
        this.req = req;
    }

    @Override
    protected void innerRun() {
        if (logger.isFineEnabled()) {
            logger.fine("Received " + req);
        }

        RaftState state = raftNode.state();

        LogEntry snapshot = req.snapshot();

        // Reply false if term < currentTerm (§5.1)
        if (req.term() < state.term()) {
            logger.warning("Stale snapshot: " + req + " received in current term: " + state.term());
            AppendFailureResponse resp = new AppendFailureResponse(raftNode.getLocalEndpoint(), state.term(), snapshot.index() + 1);
            raftNode.send(resp, req.leader());
            return;
        }

        // Transform into follower if a newer term is seen or another node wins the election of the current term
        if (req.term() > state.term() || state.role() != RaftRole.FOLLOWER) {
            // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
            logger.info("Demoting to FOLLOWER from current role: " + state.role() + ", term: " + state.term()
                    + " to new term: " + req.term() + " and leader: " + req.leader());
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

    @Override
    protected RaftEndpoint senderEndpoint() {
        return req.leader();
    }
}
