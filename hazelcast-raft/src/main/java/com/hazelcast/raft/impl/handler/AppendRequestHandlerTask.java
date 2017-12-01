package com.hazelcast.raft.impl.handler;

import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftNode.RaftNodeStatus;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.log.RaftLog;
import com.hazelcast.raft.impl.operation.TerminateRaftGroupOp;
import com.hazelcast.raft.impl.state.RaftState;

import java.util.Arrays;
import java.util.List;

import static java.lang.Math.min;
import static java.util.Collections.singletonList;

/**
 * TODO: Javadoc Pending...
 *
 */
public class AppendRequestHandlerTask extends RaftNodeAwareTask implements Runnable {
    private final AppendRequest req;

    public AppendRequestHandlerTask(RaftNode raftNode, AppendRequest req) {
        super(raftNode);
        this.req = req;
    }

    @Override
    protected void innerRun() {
        if (logger.isFineEnabled()) {
            logger.fine("Received " + req);
        }

        RaftState state = raftNode.state();

        // Reply false if term < currentTerm (§5.1)
        if (req.term() < state.term()) {
            logger.warning("Stale " + req + " received in current term: " + state.term());
            raftNode.send(createFailureResponse(state.term()), req.leader());
            return;
        }

        RaftLog raftLog = state.log();

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

        // Verify the last log entry
        if (req.prevLogIndex() > 0) {
            int lastLogIndex = raftLog.lastLogOrSnapshotIndex();
            int lastLogTerm = raftLog.lastLogOrSnapshotTerm();

            int prevLogTerm;
            if (req.prevLogIndex() == lastLogIndex) {
                prevLogTerm = lastLogTerm;
            } else {
                // Reply false if log does not contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
                LogEntry prevLog = raftLog.getLogEntry(req.prevLogIndex());
                if (prevLog == null) {
                    logger.warning("Failed to get previous log index for " + req + ", last log index: " + lastLogIndex);
                    raftNode.send(createFailureResponse(req.term()), req.leader());
                    return;
                }
                prevLogTerm = prevLog.term();
            }

            if (req.prevLogTerm() != prevLogTerm) {
                logger.warning("Previous log term of " + req + " is different than ours: " + prevLogTerm);
                raftNode.send(createFailureResponse(req.term()), req.leader());
                return;
            }
        }

        // Process any new entries
        if (req.entryCount() > 0) {
            // Delete any conflicting entries, skip any duplicates
            int lastLogIndex = raftLog.lastLogOrSnapshotIndex();

            LogEntry[] newEntries = null;
            for (int i = 0; i < req.entryCount(); i++) {
                LogEntry reqEntry = req.entries()[i];

                if (reqEntry.index() > lastLogIndex) {
                    newEntries = Arrays.copyOfRange(req.entries(), i, req.entryCount());
                    break;
                }

                LogEntry localEntry = raftLog.getLogEntry(reqEntry.index());

                assert localEntry != null : "Entry not found on log index: " + reqEntry.index() + " for " + req;

                // If an existing entry conflicts with a new one (same index but different terms),
                // delete the existing entry and all that follow it (§5.3)
                if (reqEntry.term() != localEntry.term()) {
                    List<LogEntry> truncated = raftLog.truncateEntriesFrom(reqEntry.index());
                    if (logger.isFineEnabled()) {
                        logger.warning("Truncated " + truncated.size() + " entries from entry index: " + reqEntry.index() + " => "
                                + truncated);
                    } else {
                        logger.warning("Truncated " + truncated.size() + " entries from entry index: " + reqEntry.index());
                    }

                    raftNode.invalidateFuturesFrom(reqEntry.index());
                    updateRaftNodeStatus(truncated, RaftNodeStatus.ACTIVE);

                    //                            if (entry.index <= r.configurations.latestIndex) {
                    //                                r.configurations.latest = r.configurations.committed
                    //                                r.configurations.latestIndex = r.configurations.committedIndex
                    //                            }
                    newEntries = Arrays.copyOfRange(req.entries(), i, req.entryCount());
                    break;
                }
            }

            if (newEntries != null && newEntries.length > 0) {
                // Append any new entries not already in the log
                if (logger.isFineEnabled()) {
                    logger.fine("Appending " + newEntries.length + " entries: " + Arrays.toString(newEntries));
                }

                raftLog.appendEntries(newEntries);
                updateRaftNodeStatus(singletonList(raftLog.lastLogOrSnapshotEntry()), RaftNodeStatus.TERMINATING);

                // Handle any new configuration changes
                //                        for _, newEntry := range newEntries {
                //                            r.processConfigurationLogEntry(newEntry)
                //                        }
            }
        }

        int lastLogIndex = req.prevLogIndex() + req.entryCount();

        // Update the commit index
        if (req.leaderCommitIndex() > state.commitIndex()) {
            // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            int newCommitIndex = min(req.leaderCommitIndex(), lastLogIndex);
            logger.info("Setting commit index: " + newCommitIndex);
            state.commitIndex(newCommitIndex);
            //                    if r.configurations.latestIndex <= newCommitIndex {
            //                        r.configurations.committed = r.configurations.latest
            //                        r.configurations.committedIndex = r.configurations.latestIndex
            //                    }
            raftNode.processLogs();
        }

        raftNode.updateLastAppendEntriesTimestamp();
        AppendSuccessResponse resp = new AppendSuccessResponse(raftNode.getLocalEndpoint(), state.term(), lastLogIndex);
        raftNode.send(resp, req.leader());
    }

    private void updateRaftNodeStatus(List<LogEntry> entries, RaftNodeStatus status) {
        for (LogEntry entry : entries) {
            if (entry.operation() instanceof TerminateRaftGroupOp) {
                raftNode.setStatus(status);
                return;
            }
        }
    }

    private AppendFailureResponse createFailureResponse(int term) {
        return new AppendFailureResponse(raftNode.getLocalEndpoint(), term, req.prevLogIndex() + 1);
    }

    @Override
    protected RaftEndpoint senderEndpoint() {
        return req.leader();
    }
}
