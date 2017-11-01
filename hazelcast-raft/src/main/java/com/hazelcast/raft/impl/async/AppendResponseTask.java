package com.hazelcast.raft.impl.async;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.impl.LeaderState;
import com.hazelcast.raft.impl.LogEntry;
import com.hazelcast.raft.impl.RaftLog;
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
    private final ILogger logger;

    public AppendResponseTask(RaftNode raftNode, AppendResponse response) {
        this.raftNode = raftNode;
        this.resp = response;
        this.logger = raftNode.getLogger(getClass());
    }

    @Override
    public void run() {
        RaftState state = raftNode.state();

        if (resp.term > state.term()) {
            // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
            logger.warning("Transiting to FOLLOWER, term: " + resp.term);
            state.toFollower(resp.term);
            // TODO: notify futures
            return;
        }

        if (!resp.success) {
            // If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
            logger.severe("Failure response " + resp);
            updateFollowerIndicesAndReplicateMissing(state);
            return;
        }

        logger.warning("Success response " + resp);
        // If successful: update nextIndex and matchIndex for follower (§5.3)
        updateFollowerIndicesAfterSuccess(state);

        // If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
        // set commitIndex = N (§5.3, §5.4)
        int quorumMatchIndex = findQuorumMatchIndex(state);
        int commitIndex = state.commitIndex();
        if (commitIndex == quorumMatchIndex) {
            return;
        }

        assert commitIndex < quorumMatchIndex : "Commit: " + commitIndex + ", Match: " + quorumMatchIndex;

        RaftLog raftLog = state.log();
        for (; quorumMatchIndex > commitIndex; quorumMatchIndex--) {
            LogEntry logEntry = raftLog.getEntry(quorumMatchIndex);
            if (logEntry.term() == state.term()) {
                progressCommitState(state, quorumMatchIndex);
                break;
            }
        }
    }

    private void updateFollowerIndicesAndReplicateMissing(RaftState state) {
        if (updateFollowerIndicesAfterFailure(state)) {
            raftNode.sendHeartbeat(resp.follower);
        }
    }

    private boolean updateFollowerIndicesAfterFailure(RaftState state) {
        LeaderState leaderState = state.leaderState();
        int nextIndex = leaderState.getNextIndex(resp.follower);
        int matchIndex = leaderState.getMatchIndex(resp.follower);
        int followerLastLogIndex = resp.lastLogIndex;

        if (followerLastLogIndex >= nextIndex) {
            logger.warning("Will not update follower next index. Follower: " + followerLastLogIndex + ", Next: " + nextIndex);
            return false;
        }

        if (followerLastLogIndex < matchIndex) {
            logger.severe("Will not update follower next index. Follower: " + followerLastLogIndex + ", Match: " + matchIndex);
            return false;
        }

        logger.warning("Updating next index for " + resp.follower + " to " + (followerLastLogIndex + 1));
        leaderState.setNextIndex(resp.follower, followerLastLogIndex + 1);
        return true;
    }


    private void updateFollowerIndicesAfterSuccess(RaftState state) {
        LeaderState leaderState = state.leaderState();
        int nextIndex = leaderState.getNextIndex(resp.follower);
        int matchIndex = leaderState.getMatchIndex(resp.follower);
        int followerLastLogIndex = resp.lastLogIndex;

        if (followerLastLogIndex < nextIndex) {
            logger.warning("Will not update indices for " + resp.follower + " . Follower: " + followerLastLogIndex + ", Next: " + nextIndex);
            return;
        }

        if (followerLastLogIndex < matchIndex) {
            logger.warning("Will not update indices for " + resp.follower + ". Follower: " + followerLastLogIndex + ", Match: " + matchIndex);
            return;
        }

        logger.warning("Updating indices for " + resp.follower + " to " + followerLastLogIndex + "/" +  (followerLastLogIndex + 1));
        leaderState.setMatchIndex(resp.follower, followerLastLogIndex);
        leaderState.setNextIndex(resp.follower, followerLastLogIndex + 1);
    }

    private int findQuorumMatchIndex(RaftState state) {
        LeaderState leaderState = state.leaderState();
        Collection<Integer> matchIndices = leaderState.matchIndices();
        int[] indices = new int[matchIndices.size() + 1];
        indices[0] = state.log().lastLogIndex();

        int k = 1;
        for (int index : matchIndices) {
            indices[k++] = index;
        }
        Arrays.sort(indices);

        int quorumMatchIndex = indices[(indices.length - 1) / 2];
        logger.warning("Quorum match index: " + quorumMatchIndex + ", indices: " + Arrays.toString(indices));
        return quorumMatchIndex;
    }

    private void progressCommitState(RaftState state, int commitIndex) {
        logger.severe("Commit index: " + commitIndex);
        state.commitIndex(commitIndex);
        raftNode.sendHeartbeat();
        raftNode.processLogs(commitIndex);
    }

    @Override
    public int getKey() {
        return raftNode.getStripeKey();
    }
}
