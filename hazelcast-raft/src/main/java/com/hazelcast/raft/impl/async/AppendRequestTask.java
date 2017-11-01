package com.hazelcast.raft.impl.async;

import com.hazelcast.nio.Address;
import com.hazelcast.raft.impl.LogEntry;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.RaftState;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendResponse;
import com.hazelcast.raft.impl.operation.AppendResponseOp;
import com.hazelcast.util.executor.StripedRunnable;

import java.util.Arrays;

/**
 * TODO: Javadoc Pending...
 *
 */
public class AppendRequestTask implements StripedRunnable {
    private final RaftNode raftNode;
    private final AppendRequest req;

    public AppendRequestTask(RaftNode raftNode, AppendRequest req) {
        this.raftNode = raftNode;
        this.req = req;
    }

    @Override
    public void run() {
        AppendResponse resp = new AppendResponse();
        resp.follower = raftNode.getNodeEngine().getThisAddress();
        RaftState state = raftNode.state();
        resp.term = state.term();
        resp.lastLogIndex = state.lastLogIndex();

        try {
            if (req.term < state.term()) {
                raftNode.logger.warning(
                        "Older append entries received term: " + req.term + ", current-term: " + state.term());
                return;
            }

            // Increase the term if we see a newer one, also transition to follower
            // if we ever get an appendEntries call
            if (req.term > state.term() || state.role() != RaftRole.FOLLOWER) {
                // Ensure transition to follower
                raftNode.logger.warning("Transiting to FOLLOWER, term: " + req.term);
                state.toFollower(req.term);
                resp.term = req.term;
            }

            if (!req.leader.equals(state.leader())) {
                raftNode.logger.severe("Setting leader " + req.leader);
            }
            state.leader(req.leader);

            // Verify the last log entry
            if (req.prevLogIndex > 0) {
                int lastIndex = state.lastLogIndex();
                int lastTerm = state.lastLogTerm();

                int prevLogTerm;
                if (req.prevLogIndex == lastIndex) {
                    prevLogTerm = lastTerm;

                } else {
                    LogEntry prevLog = state.getLogEntry(req.prevLogIndex);
                    if (prevLog == null) {
                        raftNode.logger.warning("Failed to get previous log " + req.prevLogIndex + ", last-index: " + lastIndex);
                        //                            resp.NoRetryBackoff = true
                        return;
                    }
                    prevLogTerm = prevLog.term();
                }

                if (req.prevLogTerm != prevLogTerm) {
                    raftNode.logger.warning("Previous log term mis-match: ours: " + prevLogTerm + ", remote: " + req.prevLogTerm);
                    //                        resp.NoRetryBackoff = true
                    return;
                }
            }

            // Process any new entries
            if (req.entries.length > 0) {
                // Delete any conflicting entries, skip any duplicates
                int lastLogIndex = state.lastLogIndex();

                LogEntry[] newEntries = null;
                for (int i = 0; i < req.entries.length; i++) {
                    LogEntry entry = req.entries[i];

                    if (entry.index() > lastLogIndex) {
                        newEntries = Arrays.copyOfRange(req.entries, i, req.entries.length);
                        break;
                    }

                    LogEntry storeEntry = state.getLogEntry(entry.index());
                    if (storeEntry == null) {
                        raftNode.logger.warning("Failed to get log entry: " + entry.index());
                        return;
                    }

                    if (entry.term() != storeEntry.term()) {
                        raftNode.logger.warning("Clearing log suffix from " + entry.index() + " to " + lastLogIndex);
                        try {
                            state.deleteLogAfter(entry.index());
                        } catch (Exception e) {
                            raftNode.logger.severe("Failed to clear log from " + entry.index() + " to " + lastLogIndex, e);
                            return;
                        }

                        //                            if (entry.index <= r.configurations.latestIndex) {
                        //                                r.configurations.latest = r.configurations.committed
                        //                                r.configurations.latestIndex = r.configurations.committedIndex
                        //                            }
                        newEntries = Arrays.copyOfRange(req.entries, i, req.entries.length);
                        break;
                    }
                }

                if (newEntries != null && newEntries.length > 0) {
                    // Append the new entries
                    try {
                        state.storeLogs(newEntries);
                    } catch (Exception e) {
                        raftNode.logger.severe("Failed to append to logs", e);
                        return;
                    }

                    // Handle any new configuration changes
                    //                        for _, newEntry := range newEntries {
                    //                            r.processConfigurationLogEntry(newEntry)
                    //                        }
                }
            }

            // Update the commit index
            if (req.leaderCommitIndex > 0 && req.leaderCommitIndex > state.commitIndex()) {
                int idx = Math.min(req.leaderCommitIndex, state.lastLogIndex());
                state.commitIndex(idx);
                //                    if r.configurations.latestIndex <= idx {
                //                        r.configurations.committed = r.configurations.latest
                //                        r.configurations.committedIndex = r.configurations.latestIndex
                //                    }
                raftNode.processLogs(idx);
            }

            // Everything went well, set success
            resp.success = true;
        } finally {
            sendResponse(resp, req.leader);
        }
    }

    private void sendResponse(AppendResponse resp, Address leader) {
        AppendResponseOp op = new AppendResponseOp(raftNode.state().name(), resp);
        raftNode.getNodeEngine().getOperationService().send(op, leader);
    }

    @Override
    public int getKey() {
        return raftNode.getStripeKey();
    }
}
