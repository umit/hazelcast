package com.hazelcast.raft.impl.async;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.impl.LogEntry;
import com.hazelcast.raft.impl.RaftLog;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.RaftState;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendResponse;
import com.hazelcast.raft.impl.operation.AppendResponseOp;
import com.hazelcast.util.executor.StripedRunnable;

import java.util.Arrays;

import static java.lang.Math.min;

/**
 * TODO: Javadoc Pending...
 *
 */
public class AppendRequestTask implements StripedRunnable {
    private final RaftNode raftNode;
    private final AppendRequest req;
    private final ILogger logger;

    public AppendRequestTask(RaftNode raftNode, AppendRequest req) {
        this.raftNode = raftNode;
        this.req = req;
        this.logger = raftNode.getLogger(getClass());
    }

    @Override
    public void run() {
        RaftState state = raftNode.state();
        RaftLog raftLog = state.log();

        AppendResponse resp = new AppendResponse();
        resp.follower = raftNode.getThisAddress();
        resp.term = state.term();
        resp.lastLogIndex = raftLog.lastLogIndex();

        logger.warning("Received " + req);

        try {
            // Reply false if term < currentTerm (§5.1)
            if (req.term < state.term()) {
                logger.warning("Older append entries received in request term: " + req.term + ", current term: " + state.term());
                return;
            }

            // Increase the term if we see a newer one, also transition to follower
            // if we ever get an appendEntries call
            if (req.term > state.term() || state.role() != RaftRole.FOLLOWER) {
                // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
                logger.warning("Transiting to FOLLOWER, request term: " + req.term + ", current term: " + state.term());
                state.toFollower(req.term);
                resp.term = req.term;
            }

            if (!req.leader.equals(state.leader())) {
                logger.severe("Setting leader " + req.leader);
                state.leader(req.leader);
            }

            // Verify the last log entry
            if (req.prevLogIndex > 0) {
                int lastLogIndex = raftLog.lastLogIndex();
                int lastLogTerm = raftLog.lastLogTerm();

                int prevLogTerm;
                if (req.prevLogIndex == lastLogIndex) {
                    prevLogTerm = lastLogTerm;
                } else {
                    // Reply false if log does not contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
                    LogEntry prevLog = raftLog.getEntry(req.prevLogIndex);
                    if (prevLog == null) {
                        logger.warning("Failed to get previous log " + req.prevLogIndex + ", last log index: " + lastLogIndex);
                        //                            resp.NoRetryBackoff = true
                        return;
                    }
                    prevLogTerm = prevLog.term();
                }

                if (req.prevLogTerm != prevLogTerm) {
                    logger.warning("Previous log term mismatch: ours: " + prevLogTerm + ", remote: " + req.prevLogTerm);
                    //                        resp.NoRetryBackoff = true
                    return;
                }
            }

            // Process any new entries
            if (req.entries.length > 0) {
                // Delete any conflicting entries, skip any duplicates
                int lastLogIndex = raftLog.lastLogIndex();

                LogEntry[] newEntries = null;
                for (int i = 0; i < req.entries.length; i++) {
                    LogEntry entry = req.entries[i];

                    if (entry.index() > lastLogIndex) {
                        newEntries = Arrays.copyOfRange(req.entries, i, req.entries.length);
                        break;
                    }

                    LogEntry storeEntry = raftLog.getEntry(entry.index());
                    if (storeEntry == null) {
                        logger.warning("Failed to get log entry: " + entry.index());
                        return;
                    }

                    // If an existing entry conflicts with a new one (same index but different terms),
                    // delete the existing entry and all that follow it (§5.3)
                    if (entry.term() != storeEntry.term()) {
                        logger.warning("Truncating log suffix from " + entry.index() + " to " + lastLogIndex);
                        raftLog.truncateEntriesAfter(entry.index());
                        raftNode.invalidateFuturesFrom(entry.index());

                        //                            if (entry.index <= r.configurations.latestIndex) {
                        //                                r.configurations.latest = r.configurations.committed
                        //                                r.configurations.latestIndex = r.configurations.committedIndex
                        //                            }
                        newEntries = Arrays.copyOfRange(req.entries, i, req.entries.length);
                        break;
                    }
                }

                if (newEntries != null && newEntries.length > 0) {
                    // Append any new entries not already in the log
                    raftLog.appendEntries(newEntries);
                    resp.lastLogIndex = raftLog.lastLogIndex();

                    // Handle any new configuration changes
                    //                        for _, newEntry := range newEntries {
                    //                            r.processConfigurationLogEntry(newEntry)
                    //                        }
                }
            }

            // Update the commit index
            if (req.leaderCommitIndex > state.commitIndex()) {
                // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
                int newCommitIndex = min(req.leaderCommitIndex, raftLog.lastLogIndex());
                state.commitIndex(newCommitIndex);
                //                    if r.configurations.latestIndex <= newCommitIndex {
                //                        r.configurations.committed = r.configurations.latest
                //                        r.configurations.committedIndex = r.configurations.latestIndex
                //                    }
                raftNode.processLogs(newCommitIndex);
            }

            // Everything went well, set success
            resp.success = true;
        } finally {
            raftNode.send(new AppendResponseOp(raftNode.state().name(), resp), req.leader);
        }
    }

    @Override
    public int getKey() {
        return raftNode.getStripeKey();
    }
}
