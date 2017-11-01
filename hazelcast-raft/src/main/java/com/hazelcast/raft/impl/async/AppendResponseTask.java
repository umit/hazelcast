package com.hazelcast.raft.impl.async;

import com.hazelcast.raft.impl.LeaderState;
import com.hazelcast.raft.impl.LogEntry;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftState;
import com.hazelcast.raft.impl.dto.AppendResponse;
import com.hazelcast.util.executor.StripedRunnable;

import java.util.Arrays;
import java.util.Collection;

/**
 * TODO: Javadoc Pending...
 *
 */
public class AppendResponseTask implements StripedRunnable {
    private final RaftNode raftNode;
    private final AppendResponse resp;

    public AppendResponseTask(RaftNode raftNode, AppendResponse response) {
        this.raftNode = raftNode;
        this.resp = response;
    }

    @Override
    public void run() {
        RaftState state = raftNode.state();

        // Check for a newer term, stop running
        if (resp.term > state.term()) {
            // TODO: ?
//        if (resp.term > req.term()) {
//                            r.handleStaleTerm(s)
            return;
        }

        // Abort pipeline if not successful
        if (!resp.success) {
            raftNode.logger.severe("Failure response " + resp);
            // TODO: handle?
            return;
        }

        raftNode.logger.warning("Success response " + resp);

        int followerLastLogIndex = resp.lastLogIndex;
        LeaderState leaderState = state.leaderState();
        leaderState.matchIndex(resp.follower, followerLastLogIndex);
        leaderState.nextIndex(resp.follower, followerLastLogIndex + 1);

        int commitIndex = state.commitIndex();
        int term = state.term();

        Collection<Integer> matchIndices = leaderState.matchIndices();
        int[] indices = new int[matchIndices.size() + 1];
        indices[0] = state.lastLogIndex();

        int k = 1;
        for (int index : matchIndices) {
            indices[k++] = index;
        }
        Arrays.sort(indices);

        int quorumMatchIndex = indices[(indices.length - 1) / 2];
        raftNode.logger.warning("Quorum match index: " + quorumMatchIndex + ", indices: " + Arrays.toString(indices));
        if (commitIndex == quorumMatchIndex) {
            return;
        }
        assert commitIndex < quorumMatchIndex : "Commit: " + commitIndex + ", Match: " + quorumMatchIndex;

        for (; quorumMatchIndex > commitIndex; quorumMatchIndex--) {
            LogEntry logEntry = state.getLogEntry(quorumMatchIndex);
            if (logEntry.term() == term) {
                progressCommitState(state, quorumMatchIndex);
                break;
            }
        }
    }

    private void progressCommitState(RaftState state, int commitIndex) {
        raftNode.logger.severe("Commit index: " + commitIndex);
        state.commitIndex(commitIndex);
        raftNode.sendHeartbeat();
        raftNode.processLogs(commitIndex);
    }

    @Override
    public int getKey() {
        return raftNode.getStripeKey();
    }
}
