package com.hazelcast.raft.impl.async;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.impl.LeaderState;
import com.hazelcast.raft.impl.LogEntry;
import com.hazelcast.raft.impl.RaftLog;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.RaftState;
import com.hazelcast.raft.impl.dto.AppendResponse;
import com.hazelcast.util.executor.StripedRunnable;

import java.util.Arrays;
import java.util.Collection;

import static java.util.Arrays.sort;

/**
 * TODO: Javadoc Pending...
 *
 */
public class AppendResponseHandlerTask implements StripedRunnable {
    private final RaftNode raftNode;
    private final AppendResponse resp;
    private final ILogger logger;

    public AppendResponseHandlerTask(RaftNode raftNode, AppendResponse response) {
        this.raftNode = raftNode;
        this.resp = response;
        this.logger = raftNode.getLogger(getClass());
    }

    @Override
    public void run() {
        RaftState state = raftNode.state();
        if (state.role() != RaftRole.LEADER) {
            logger.severe("Ignored " + resp + ". We are not LEADER anymore.");
            return;
        }

        if (resp.term > state.term()) {
            // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
            logger.warning("Transiting to FOLLOWER, response term: " + resp.term + ", current term: " + state.term());
            state.toFollower(resp.term);
            return;
        }

        if (!resp.success) {
            // If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
            logger.severe("Failure response " + resp);
            if (updateFollowerIndicesAfterFailure(state)) {
                raftNode.sendAppendRequest(resp.follower);
            }
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
            // Only log entries from the leader’s current term are committed by counting replicas; once an entry
            // from the current term has been committed in this way, then all prior entries are committed indirectly
            // because of the Log Matching Property.
            LogEntry logEntry = raftLog.getEntry(quorumMatchIndex);
            if (logEntry.term() == state.term()) {
                progressCommitState(state, quorumMatchIndex);
                break;
            } else {
                logger.severe("term: " + state.term() + " => " + logEntry);
            }
        }
    }

    private boolean updateFollowerIndicesAfterFailure(RaftState state) {
        LeaderState leaderState = state.leaderState();
        int nextIndex = leaderState.getNextIndex(resp.follower);
        int matchIndex = leaderState.getMatchIndex(resp.follower);
        int followerLastLogIndex = resp.lastLogIndex;

        if (followerLastLogIndex >= nextIndex) {
            logger.warning("Will not update next index for follower: " + resp.follower + " . follower last log index: "
                    + followerLastLogIndex + ", next index: " + nextIndex);
            return false;
        }

        if (followerLastLogIndex < matchIndex) {
            logger.warning("Will not update next index for follower: " + resp.follower + " . follower last log index: "
                    + followerLastLogIndex + ", match index: " + matchIndex);
            return false;
        }

        logger.warning("Setting new next index: " + (followerLastLogIndex + 1) + " for follower: " + resp.follower);
        leaderState.setNextIndex(resp.follower, followerLastLogIndex + 1);
        return true;
    }

    private void updateFollowerIndicesAfterSuccess(RaftState state) {
        LeaderState leaderState = state.leaderState();
        int nextIndex = leaderState.getNextIndex(resp.follower);
        int matchIndex = leaderState.getMatchIndex(resp.follower);
        int followerLastLogIndex = resp.lastLogIndex;

        if (followerLastLogIndex > matchIndex) {
            logger.warning("Setting new match index: " + followerLastLogIndex + " for follower: " + resp.follower);
            leaderState.setMatchIndex(resp.follower, followerLastLogIndex);
        } else if (followerLastLogIndex < matchIndex) {
            logger.warning("Will not update match index for follower: " + resp.follower + ". follower last log index: "
                    + followerLastLogIndex + ", match index: " + matchIndex);
        }

        if (followerLastLogIndex > nextIndex) {
            logger.warning("Setting new next index: " + (followerLastLogIndex + 1) + " for follower: " + resp.follower);
            leaderState.setNextIndex(resp.follower, followerLastLogIndex + 1);
        } else if (followerLastLogIndex < nextIndex) {
            logger.warning("Will not update next index for follower: " + resp.follower + " . follower last log index: "
                    + followerLastLogIndex + ", next index: " + nextIndex);
        }
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
        sort(indices);

        int quorumMatchIndex = indices[(indices.length - 1) / 2];
        logger.warning("Quorum match index: " + quorumMatchIndex + ", indices: " + Arrays.toString(indices));
        return quorumMatchIndex;
    }

    private void progressCommitState(RaftState state, int commitIndex) {
        logger.severe("Setting commit index: " + commitIndex);
        state.commitIndex(commitIndex);
        raftNode.broadcastAppendRequest();
        raftNode.processLogs();
    }

    @Override
    public int getKey() {
        return raftNode.getStripeKey();
    }
}
