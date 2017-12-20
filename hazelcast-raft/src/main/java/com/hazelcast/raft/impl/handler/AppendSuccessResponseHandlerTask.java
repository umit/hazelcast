package com.hazelcast.raft.impl.handler;

import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.log.RaftLog;
import com.hazelcast.raft.impl.state.LeaderState;
import com.hazelcast.raft.impl.state.RaftState;

import java.util.Arrays;
import java.util.Collection;

import static java.util.Arrays.sort;

/**
 * TODO: Javadoc Pending...
 *
 */
public class AppendSuccessResponseHandlerTask extends AbstractResponseHandlerTask {
    private final AppendSuccessResponse resp;

    public AppendSuccessResponseHandlerTask(RaftNodeImpl raftNode, AppendSuccessResponse response) {
        super(raftNode);
        this.resp = response;
    }

    @Override
    protected void handleResponse() {
        RaftState state = raftNode.state();

        if (state.role() != RaftRole.LEADER) {
            logger.warning("Ignored " + resp + ". We are not LEADER anymore.");
            return;
        }

        assert resp.term() <= state.term() : "Invalid " + resp + " for current term: " + state.term();

        if (logger.isFineEnabled()) {
            logger.fine("Received " + resp);
        }

        // If successful: update nextIndex and matchIndex for follower (§5.3)
        updateFollowerIndices(state);

        // If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
        // set commitIndex = N (§5.3, §5.4)
        int quorumMatchIndex = findQuorumMatchIndex(state);
        int commitIndex = state.commitIndex();
        RaftLog raftLog = state.log();
        for (; quorumMatchIndex > commitIndex; quorumMatchIndex--) {
            // Only log entries from the leader’s current term are committed by counting replicas; once an entry
            // from the current term has been committed in this way, then all prior entries are committed indirectly
            // because of the Log Matching Property.
            LogEntry entry = raftLog.getLogEntry(quorumMatchIndex);
            if (entry.term() == state.term()) {
                commitEntries(state, quorumMatchIndex);
                break;
            } else if (logger.isFineEnabled()) {
                logger.fine("Cannot commit " + entry + " since an entry from the current term: " + state.term() + " is needed.");
            }
        }
    }

    private void updateFollowerIndices(RaftState state) {
        RaftEndpoint follower = resp.follower();
        LeaderState leaderState = state.leaderState();
        int matchIndex = leaderState.getMatchIndex(follower);
        int followerLastLogIndex = resp.lastLogIndex();

        if (followerLastLogIndex > matchIndex) {
            int newNextIndex = followerLastLogIndex + 1;
            logger.info("Updating match index: " + followerLastLogIndex + " and next index: " + newNextIndex
                    + " for follower: " + follower);
            leaderState.setMatchIndex(follower, followerLastLogIndex);
            leaderState.setNextIndex(follower, newNextIndex);
        } else if (followerLastLogIndex < matchIndex) {
            logger.warning("Will not update match index for follower: " + follower + ". follower last log index: "
                    + followerLastLogIndex + ", match index: " + matchIndex);
        }
    }

    private int findQuorumMatchIndex(RaftState state) {
        LeaderState leaderState = state.leaderState();
        Collection<Integer> matchIndices = leaderState.matchIndices();

        int[] indices;
        int k;

        // if the leader is leaving, it should not count its vote for quorum...
        if (raftNode.state().isKnownEndpoint(raftNode.getLocalEndpoint())) {
            indices = new int[matchIndices.size() + 1];
            indices[0] = state.log().lastLogOrSnapshotIndex();
            k = 1;
        } else {
            indices = new int[matchIndices.size()];
            k = 0;
        }

        for (int index : matchIndices) {
            indices[k++] = index;
        }
        sort(indices);

        int quorumMatchIndex = indices[(indices.length - 1) / 2];
        if (logger.isFineEnabled()) {
            logger.fine("Quorum match index: " + quorumMatchIndex + ", indices: " + Arrays.toString(indices));
        }

        return quorumMatchIndex;
    }

    private void commitEntries(RaftState state, int commitIndex) {
        logger.info("Setting commit index: " + commitIndex);
        state.commitIndex(commitIndex);
        raftNode.broadcastAppendRequest();
        raftNode.processLogEntries();
    }

    @Override
    protected RaftEndpoint senderEndpoint() {
        return resp.follower();
    }
}
