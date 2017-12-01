package com.hazelcast.raft.impl;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.RaftOperation;
import com.hazelcast.raft.exception.LeaderDemotedException;
import com.hazelcast.raft.exception.StaleAppendRequestException;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.raft.impl.dto.InstallSnapshot;
import com.hazelcast.raft.impl.dto.PreVoteRequest;
import com.hazelcast.raft.impl.dto.PreVoteResponse;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.raft.impl.handler.AppendFailureResponseHandlerTask;
import com.hazelcast.raft.impl.handler.AppendRequestHandlerTask;
import com.hazelcast.raft.impl.handler.AppendSuccessResponseHandlerTask;
import com.hazelcast.raft.impl.handler.InstallSnapshotHandlerTask;
import com.hazelcast.raft.impl.handler.PreVoteRequestHandlerTask;
import com.hazelcast.raft.impl.handler.PreVoteResponseHandlerTask;
import com.hazelcast.raft.impl.handler.PreVoteTask;
import com.hazelcast.raft.impl.handler.ReplicateTask;
import com.hazelcast.raft.impl.handler.VoteRequestHandlerTask;
import com.hazelcast.raft.impl.handler.VoteResponseHandlerTask;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.log.RaftLog;
import com.hazelcast.raft.impl.operation.RestoreSnapshotOp;
import com.hazelcast.raft.impl.operation.TakeSnapshotOp;
import com.hazelcast.raft.impl.operation.TerminateRaftGroupOp;
import com.hazelcast.raft.impl.state.LeaderState;
import com.hazelcast.raft.impl.state.RaftState;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.util.Clock;
import com.hazelcast.util.RandomPicker;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.impl.RaftNode.RaftNodeStatus.ACTIVE;
import static com.hazelcast.raft.impl.RaftNode.RaftNodeStatus.TERMINATED;
import static com.hazelcast.raft.impl.RaftNode.RaftNodeStatus.TERMINATING;
import static java.lang.Math.min;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftNode {

    public enum RaftNodeStatus {
        ACTIVE, TERMINATING, TERMINATED
    }

    private static final long SNAPSHOT_TASK_PERIOD_IN_SECONDS = 1;
    private static final int LEADER_ELECTION_TIMEOUT_RANGE = 1000;

    private final String serviceName;
    private final ILogger logger;
    private final RaftState state;
    private final RaftIntegration raftIntegration;
    private final RaftEndpoint localEndpoint;
    private final Long2ObjectHashMap<SimpleCompletableFuture> futures = new Long2ObjectHashMap<SimpleCompletableFuture>();

    private final long heartbeatPeriodInMillis;
    private final int leaderElectionTimeout;
    private final int maxUncommittedEntryCount;
    private final int appendRequestMaxEntryCount;
    private final int commitIndexAdvanceCountToSnapshot;

    private long lastAppendEntriesTimestamp;
    private RaftNodeStatus status = ACTIVE;

    public RaftNode(String serviceName, String name, RaftEndpoint localEndpoint, Collection<RaftEndpoint> endpoints,
                    RaftConfig raftConfig, RaftIntegration raftIntegration) {
        this.serviceName = serviceName;
        this.raftIntegration = raftIntegration;
        this.localEndpoint = localEndpoint;
        this.state = new RaftState(name, localEndpoint, endpoints);
        this.logger = getLogger(getClass());
        this.maxUncommittedEntryCount = raftConfig.getUncommittedEntryCountToRejectNewAppends();
        this.appendRequestMaxEntryCount = raftConfig.getAppendRequestMaxEntryCount();
        this.commitIndexAdvanceCountToSnapshot = raftConfig.getCommitIndexAdvanceCountToSnapshot();
        this.leaderElectionTimeout = (int) raftConfig.getLeaderElectionTimeoutInMillis();
        this.heartbeatPeriodInMillis = raftConfig.getLeaderHeartbeatPeriodInMillis();
    }

    public ILogger getLogger(Class clazz) {
        String name = state.name();
        return raftIntegration.getLogger(clazz.getName() + "(" + name + ")");
    }

    public RaftEndpoint getLocalEndpoint() {
        return localEndpoint;
    }

    public RaftNodeStatus getStatus() {
        return status;
    }

    public boolean isTerminated() {
        return status == TERMINATED;
    }

    public void setStatus(RaftNodeStatus status) {
        if (this.status == TERMINATED) {
            throw new IllegalStateException("Cannot set status: " + status + " since it is already TERMINATED");
        }

        this.status = status;
        logger.info("Status is set to: " + status);
    }

    public long getLeaderElectionTimeoutInMillis() {
        return RandomPicker.getInt(leaderElectionTimeout, leaderElectionTimeout + LEADER_ELECTION_TIMEOUT_RANGE);
    }

    public boolean shouldAllowNewAppends() {
        int lastLogIndex = state.log().lastLogOrSnapshotIndex();
        return status == ACTIVE && (lastLogIndex - state.commitIndex() < maxUncommittedEntryCount);
    }

    public void send(PreVoteRequest request, RaftEndpoint target) {
        raftIntegration.send(request, target);
    }

    public void send(PreVoteResponse response, RaftEndpoint target) {
        raftIntegration.send(response, target);
    }

    public void send(VoteRequest request, RaftEndpoint target) {
        raftIntegration.send(request, target);
    }

    public void send(VoteResponse response, RaftEndpoint target) {
        raftIntegration.send(response, target);
    }

    public void send(AppendRequest request, RaftEndpoint target) {
        raftIntegration.send(request, target);
    }

    public void send(AppendSuccessResponse response, RaftEndpoint target) {
        raftIntegration.send(response, target);
    }

    public void send(AppendFailureResponse response, RaftEndpoint target) {
        raftIntegration.send(response, target);
    }

    public void start() {
        if (!raftIntegration.isJoined()) {
            raftIntegration.schedule(new Runnable() {
                @Override
                public void run() {
                    start();
                }
            }, 500, TimeUnit.MILLISECONDS);
            return;
        }

        logger.info("Starting raft node: " + localEndpoint + " for raft cluster: " + state.name()
                + " with members[" + state.memberCount() + "]: " + state.members());
        raftIntegration.execute(new PreVoteTask(this));

        scheduleLeaderFailureDetection();
        scheduleSnapshot();
    }

    private void scheduleSnapshot() {
        schedule(new SnapshotTask(), TimeUnit.SECONDS.toMillis(SNAPSHOT_TASK_PERIOD_IN_SECONDS));
    }

    private void scheduleLeaderFailureDetection() {
        schedule(new LeaderFailureDetectionTask(), getLeaderElectionTimeoutInMillis());
    }

    public void scheduleHeartbeat() {
        schedule(new HeartbeatTask(), heartbeatPeriodInMillis);
    }

    public int getStripeKey() {
        return state.name().hashCode();
    }


    public void broadcastAppendRequest() {
        for (RaftEndpoint follower : state.remoteMembers()) {
            sendAppendRequest(follower);
        }
        updateLastAppendEntriesTimestamp();
    }

    public void sendAppendRequest(RaftEndpoint follower) {
        RaftLog raftLog = state.log();
        LeaderState leaderState = state.leaderState();

        int nextIndex = leaderState.getNextIndex(follower);

        if (nextIndex <= raftLog.snapshotIndex()) {
            InstallSnapshot installSnapshot = new InstallSnapshot(localEndpoint, state.term(), raftLog.snapshot());
            if (logger.isFineEnabled()) {
                logger.fine("Sending " + installSnapshot + " to " + follower + " since next index: " + nextIndex
                        + " <= snapshot index: " + raftLog.snapshotIndex());
            }
            raftIntegration.send(installSnapshot, follower);
            return;
        }

        LogEntry prevEntry;
        LogEntry[] entries;
        if (nextIndex > 1) {
            int prevEntryIndex = nextIndex - 1;
            prevEntry = (prevEntryIndex == raftLog.snapshotIndex()) ? raftLog.snapshot() : raftLog.getLogEntry(prevEntryIndex);

            int matchIndex = leaderState.getMatchIndex(follower);
            if (matchIndex == 0 && nextIndex > (matchIndex + 1)) {
                // Until the leader has discovered where it and the follower's logs match,
                // the leader can send AppendEntries with no entries (like heartbeats) to save bandwidth.
                entries = new LogEntry[0];
            } else if (nextIndex <= raftLog.lastLogOrSnapshotIndex()) {
                // Then, once the matchIndex immediately precedes the nextIndex,
                // the leader should begin to send the actual entries
                int end = min(nextIndex + appendRequestMaxEntryCount, raftLog.lastLogOrSnapshotIndex());
                entries = raftLog.getEntriesBetween(nextIndex, end);
            } else {
                entries = new LogEntry[0];
            }
        } else if (nextIndex == 1 && raftLog.lastLogOrSnapshotIndex() > 0) {
            prevEntry = new LogEntry();
            int end = min(nextIndex + appendRequestMaxEntryCount, raftLog.lastLogOrSnapshotIndex());
            entries = raftLog.getEntriesBetween(nextIndex, end);
        } else {
            prevEntry = new LogEntry();
            entries = new LogEntry[0];
        }

        assert prevEntry != null : "Follower: " + follower + ", next index: " + nextIndex;

        AppendRequest appendRequest = new AppendRequest(getLocalEndpoint(), state.term(), prevEntry.term(), prevEntry.index(),
                state.commitIndex(), entries);

        if (logger.isFineEnabled()) {
            logger.fine("Sending " + appendRequest + " to " + follower + " with next index: " + nextIndex);
        }

        send(appendRequest, follower);
    }

    // If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
    public void processLogs() {
        // Reject logs we've applied already
        int commitIndex = state.commitIndex();
        int lastApplied = state.lastApplied();

        if (commitIndex == lastApplied) {
            return;
        }

        assert commitIndex > lastApplied : "commit index: " + commitIndex + " cannot be smaller than last applied: " + lastApplied;

        // Apply all the preceding logs
        RaftLog raftLog = state.log();
        for (int idx = state.lastApplied() + 1; idx <= commitIndex; idx++) {
            LogEntry entry = raftLog.getLogEntry(idx);
            if (entry == null) {
                String msg = "Failed to get log entry at index: " + idx;
                logger.severe(msg);
                throw new AssertionError(msg);
            }

            processLog(entry);

            // Update the lastApplied index
            state.lastApplied(idx);
        }

        assert status != TERMINATED || commitIndex == raftLog.lastLogOrSnapshotIndex() :
                "commit index: " + commitIndex + " must be equal to " + raftLog.lastLogOrSnapshotIndex() + " on termination.";
    }

    private void processLog(LogEntry entry) {
        if (logger.isFineEnabled()) {
            logger.fine("Processing " + entry);
        }

        SimpleCompletableFuture future = futures.remove(entry.index());
        Object response = null;
        RaftOperation operation = entry.operation();
        if (operation instanceof TerminateRaftGroupOp) {
            assert status == TERMINATING;
            setStatus(TERMINATED);
        } else {
            response = raftIntegration.runOperation(operation, entry.index());
        }
        if (future != null) {
            future.setResult(response);
        }
    }

    public void updateLastAppendEntriesTimestamp() {
        lastAppendEntriesTimestamp = Clock.currentTimeMillis();
    }

    public long lastAppendEntriesTimestamp() {
        return lastAppendEntriesTimestamp;
    }

    public RaftState state() {
        return state;
    }

    public void execute(Runnable task) {
        raftIntegration.execute(task);
    }

    public void schedule(Runnable task, long delayInMillis) {
        if (status == TERMINATED) {
            return;
        }
        raftIntegration.schedule(task, delayInMillis, TimeUnit.MILLISECONDS);
    }

    public void handlePreVoteRequest(PreVoteRequest request) {
        execute(new PreVoteRequestHandlerTask(this, request));
    }

    public void handlePreVoteResponse(PreVoteResponse response) {
        execute(new PreVoteResponseHandlerTask(this, response));
    }

    public void handleVoteRequest(VoteRequest request) {
        execute(new VoteRequestHandlerTask(this, request));
    }

    public void handleVoteResponse(VoteResponse response) {
        execute(new VoteResponseHandlerTask(this, response));
    }

    public void handleAppendRequest(AppendRequest request) {
        execute(new AppendRequestHandlerTask(this, request));
    }

    public void handleAppendResponse(AppendSuccessResponse response) {
        execute(new AppendSuccessResponseHandlerTask(this, response));
    }

    public void handleAppendResponse(AppendFailureResponse response) {
        execute(new AppendFailureResponseHandlerTask(this, response));
    }

    public void handleInstallSnapshot(InstallSnapshot request) {
        execute(new InstallSnapshotHandlerTask(this, request));
    }

    public void registerFuture(int entryIndex, SimpleCompletableFuture future) {
        SimpleCompletableFuture f = futures.put(entryIndex, future);
        assert f == null : "Future object is already registered for entry index: " + entryIndex;
    }

    // entryIndex is inclusive
    public void invalidateFuturesFrom(int entryIndex) {
        int count = 0;
        Iterator<Map.Entry<Long, SimpleCompletableFuture>> iterator = futures.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, SimpleCompletableFuture> entry = iterator.next();
            long index = entry.getKey();
            if (index >= entryIndex) {
                entry.getValue().setResult(new LeaderDemotedException(localEndpoint, state.leader()));
                iterator.remove();
                count++;
            }
        }

        logger.warning("Invalidated " + count + " futures from log index: " + entryIndex);
    }

    private void invalidateFuturesUntil(int entryIndex) {
        int count = 0;
        Iterator<Map.Entry<Long, SimpleCompletableFuture>> iterator = futures.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, SimpleCompletableFuture> entry = iterator.next();
            long index = entry.getKey();
            if (index <= entryIndex) {
                entry.getValue().setResult(new StaleAppendRequestException(state.leader()));
                iterator.remove();
                count++;
            }
        }

        logger.warning("Invalidated " + count + " futures until log index: " + entryIndex);
    }

    public void takeSnapshotIfCommitIndexAdvanced() {
        int commitIndex = state.commitIndex();
        if (status == TERMINATED || (commitIndex - state.log().snapshotIndex()) < commitIndexAdvanceCountToSnapshot) {
            return;
        }

        RaftLog log = state.log();
        Object snapshot = raftIntegration.runOperation(new TakeSnapshotOp(serviceName, state.name()), commitIndex);
        if (snapshot instanceof Throwable) {
            Throwable t = (Throwable) snapshot;
            logger.severe("Could not take snapshot from service '" + serviceName + "', commit index: " + commitIndex, t);
            return;
        }
        RestoreSnapshotOp snapshotOp = new RestoreSnapshotOp(serviceName, state.name(), commitIndex, snapshot);
        LogEntry committedEntry = log.getLogEntry(commitIndex);
        LogEntry snapshotLogEntry = new LogEntry(committedEntry.term(), commitIndex, snapshotOp);
        log.setSnapshot(snapshotLogEntry);

        logger.info("Snapshot: "  + snapshotLogEntry + " is taken.");
    }

    public boolean installSnapshot(LogEntry snapshot) {
        int commitIndex = state.commitIndex();
        if (commitIndex > snapshot.index()) {
            logger.warning("Ignored stale snapshot: " + snapshot + ". commit index: " + commitIndex);
            return false;
        } else if (commitIndex == snapshot.index()) {
            logger.warning("Ignored snapshot: " + snapshot + " since commit index is same.");
            return false;
        }

        state.commitIndex(snapshot.index());
        List<LogEntry> truncated = state.log().setSnapshot(snapshot);
        if (logger.isFineEnabled()) {
            logger.fine(truncated.size() + " entries are truncated to install snapshot: " + snapshot + " => " + truncated);
        } else if (truncated.size() > 0) {
            logger.info(truncated.size() + " entries are truncated to install snapshot: " + snapshot);
        }

        raftIntegration.runOperation(snapshot.operation(), snapshot.index());
        state.lastApplied(snapshot.index());

        invalidateFuturesUntil(snapshot.index());

        logger.info("Snapshot: " + snapshot + " is installed.");

        return true;
    }

    public ICompletableFuture replicate(RaftOperation operation) {
        SimpleCompletableFuture resultFuture = raftIntegration.newCompletableFuture();
        raftIntegration.execute(new ReplicateTask(this, operation, resultFuture));
        return resultFuture;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void printMemberState() {
        StringBuilder sb = new StringBuilder("\n\nRaft Members {")
                .append("name: ").append(state.name())
                .append(", size:").append(state.memberCount())
                .append(", term:").append(state.term())
                .append("} [");

        for (RaftEndpoint endpoint : state.members()) {
            sb.append("\n\t").append(endpoint.getAddress()).append(" - ").append(endpoint.getUid());
            if (localEndpoint.equals(endpoint)) {
                sb.append(" - ").append(state.role()).append(" this");
            } else if (endpoint.equals(state.leader())) {
                sb.append(" - ").append(RaftRole.LEADER);
            }
        }
        sb.append("\n]\n");
        logger.info(sb.toString());
    }

    private class HeartbeatTask implements Runnable {
        @Override
        public void run() {
            if (state.role() == RaftRole.LEADER) {
                if (lastAppendEntriesTimestamp < Clock.currentTimeMillis() - heartbeatPeriodInMillis) {
                    broadcastAppendRequest();
                }

                scheduleHeartbeat();
            }
        }
    }

    private class LeaderFailureDetectionTask implements Runnable {
        @Override
        public void run() {
            try {
                RaftEndpoint leader = state.leader();
                if (leader == null) {
                    if (state.role() == RaftRole.FOLLOWER) {
                        logger.warning("We are FOLLOWER and there is no current leader. Will start new election round...");
                        runPreVoteTask();
                    }
                } else if (!raftIntegration.isReachable(leader)) {
                    logger.warning("Current leader " + leader + " is not reachable. Will start new election round...");
                    state.leader(null);
                    printMemberState();
                    runPreVoteTask();
                }
            } finally {
                scheduleLeaderFailureDetection();
            }
        }

        private void runPreVoteTask() {
            if (state.preCandidateState() == null) {
                new PreVoteTask(RaftNode.this).run();
            }
        }
    }

    private class SnapshotTask implements Runnable {
        @Override
        public void run() {
            try {
                if (state.role() == RaftRole.LEADER || state.role() == RaftRole.FOLLOWER) {
                    takeSnapshotIfCommitIndexAdvanced();
                }
            } finally {
                scheduleSnapshot();
            }
        }
    }
}
