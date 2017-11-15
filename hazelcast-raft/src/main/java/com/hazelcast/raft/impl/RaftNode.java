package com.hazelcast.raft.impl;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.exception.LeaderDemotedException;
import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.RaftOperation;
import com.hazelcast.raft.exception.StaleAppendRequestException;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.raft.impl.dto.InstallSnapshot;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.raft.impl.handler.AppendFailureResponseHandlerTask;
import com.hazelcast.raft.impl.handler.AppendRequestHandlerTask;
import com.hazelcast.raft.impl.handler.AppendSuccessResponseHandlerTask;
import com.hazelcast.raft.impl.handler.InstallSnapshotHandlerTask;
import com.hazelcast.raft.impl.handler.LeaderElectionTask;
import com.hazelcast.raft.impl.handler.ReplicateTask;
import com.hazelcast.raft.impl.handler.VoteRequestHandlerTask;
import com.hazelcast.raft.impl.handler.VoteResponseHandlerTask;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.log.RaftLog;
import com.hazelcast.raft.impl.operation.RestoreSnapshotOp;
import com.hazelcast.raft.impl.operation.TakeSnapshotOp;
import com.hazelcast.raft.impl.state.LeaderState;
import com.hazelcast.raft.impl.state.RaftState;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.raft.impl.util.StripedRunnableAdaptor;
import com.hazelcast.spi.TaskScheduler;
import com.hazelcast.util.Clock;
import com.hazelcast.util.RandomPicker;
import com.hazelcast.util.collection.Long2ObjectHashMap;
import com.hazelcast.util.executor.StripedExecutor;
import com.hazelcast.util.executor.StripedRunnable;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.lang.Math.min;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftNode {

    private static final long SNAPSHOT_TASK_PERIOD_IN_SECONDS = 1;
    private static final int LEADER_ELECTION_TIMEOUT_RANGE = 1000;

    private final String serviceName;
    private final ILogger logger;
    private final RaftState state;
    private final StripedExecutor executor;
    private final RaftIntegration raftIntegration;
    private final RaftEndpoint localEndpoint;
    private final TaskScheduler taskScheduler;
    private final Long2ObjectHashMap<SimpleCompletableFuture> futures = new Long2ObjectHashMap<SimpleCompletableFuture>();

    private final long heartbeatPeriodInMillis;
    private final int leaderElectionTimeout;
    private final int maxUncommittedEntryCount;
    private final int appendRequestMaxEntryCount;
    private final int commitIndexAdvanceCountToSnapshot;

    private long lastAppendEntriesTimestamp;

    public RaftNode(String serviceName, String name, RaftEndpoint localEndpoint, Collection<RaftEndpoint> endpoints,
                    RaftConfig raftConfig, RaftIntegration raftIntegration, StripedExecutor executor) {
        this.serviceName = serviceName;
        this.raftIntegration = raftIntegration;
        this.executor = executor;
        this.localEndpoint = localEndpoint;
        this.state = new RaftState(name, localEndpoint, endpoints);
        this.taskScheduler = raftIntegration.getTaskScheduler();
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

    public long getLeaderElectionTimeoutInMillis() {
        return RandomPicker.getInt(leaderElectionTimeout, leaderElectionTimeout + LEADER_ELECTION_TIMEOUT_RANGE);
    }

    public boolean shouldAllowNewAppends() {
        int lastLogIndex = state.log().lastLogOrSnapshotIndex();
        return (lastLogIndex - state.commitIndex() < maxUncommittedEntryCount);
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
            taskScheduler.schedule(new Runnable() {
                @Override
                public void run() {
                    start();
                }
            }, 500, TimeUnit.MILLISECONDS);
            return;
        }

        logger.info("Starting raft node: " + localEndpoint + " for raft cluster: " + state.name()
                + " with members[" + state.memberCount() + "]: " + state.members());
        executor.execute(new LeaderElectionTask(this));

        scheduleLeaderFailureDetection();
        schedulePeriodicSnapshotTask();
    }

    private void schedulePeriodicSnapshotTask() {
        taskScheduler.scheduleWithRepetition(new Runnable() {
            @Override
            public void run() {
                executor.execute(new SnapshotTask());
            }
        }, SNAPSHOT_TASK_PERIOD_IN_SECONDS, SNAPSHOT_TASK_PERIOD_IN_SECONDS, TimeUnit.SECONDS);
    }

    private void scheduleLeaderFailureDetection() {
        taskScheduler.schedule(new Runnable() {
            @Override
            public void run() {
                executor.execute(new LeaderFailureDetectionTask());
            }
        }, getLeaderElectionTimeoutInMillis(), TimeUnit.MILLISECONDS);
    }

    public int getStripeKey() {
        return state.name().hashCode();
    }

    public void startPeriodicHeartbeatTask() {
        executor.execute(new HeartbeatTask());
    }

    public void broadcastAppendRequest() {
        for (RaftEndpoint follower : state.remoteMembers()) {
            sendAppendRequest(follower);
        }
        lastAppendEntriesTimestamp = Clock.currentTimeMillis();
    }

    public void sendAppendRequest(RaftEndpoint follower) {
        RaftLog raftLog = state.log();
        LeaderState leaderState = state.leaderState();

        int nextIndex = leaderState.getNextIndex(follower);

        if (nextIndex < raftLog.snapshotIndex()) {
            raftIntegration.send(new InstallSnapshot(localEndpoint, raftLog.snapshot()), follower);
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
    }

    private void processLog(LogEntry entry) {
        if (logger.isFineEnabled()) {
            logger.fine("Processing " + entry);
        }

        SimpleCompletableFuture future = futures.remove(entry.index());
        Object response = raftIntegration.runOperation(entry.operation(), entry.index());
        if (future != null) {
            future.setResult(response);
        }
    }

    public RaftState state() {
        return state;
    }

    public TaskScheduler taskScheduler() {
        return taskScheduler;
    }

    public void execute(StripedRunnable task) {
        assert task.getKey() == getStripeKey() : "Task-key: " + task.getKey() + ", Node-key: " + getStripeKey();
        executor.execute(task);
    }

    public void execute(Runnable task) {
        if (task instanceof StripedRunnable) {
            execute((StripedRunnable) task);
        } else {
            executor.execute(new StripedRunnableAdaptor(task, getStripeKey()));
        }
    }

    public void handleVoteRequest(VoteRequest request) {
        executor.execute(new VoteRequestHandlerTask(this, request));
    }

    public void handleVoteResponse(VoteResponse response) {
        executor.execute(new VoteResponseHandlerTask(this, response));
    }

    public void handleAppendRequest(AppendRequest request) {
        executor.execute(new AppendRequestHandlerTask(this, request));
    }

    public void handleAppendResponse(AppendSuccessResponse response) {
        executor.execute(new AppendSuccessResponseHandlerTask(this, response));
    }

    public void handleAppendResponse(AppendFailureResponse response) {
        executor.execute(new AppendFailureResponseHandlerTask(this, response));
    }

    public void handleInstallSnapshot(InstallSnapshot request) {
        executor.execute(new InstallSnapshotHandlerTask(this, request));
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
        if (commitIndexAdvanceCountToSnapshot == Integer.MAX_VALUE) {
            return;
        }
        int commitIndex = state.commitIndex();
        if ((commitIndex - state.log().snapshotIndex()) < commitIndexAdvanceCountToSnapshot) {
            return;
        }

        Object snapshot = raftIntegration.runOperation(new TakeSnapshotOp(serviceName, state.name()), commitIndex);
        if (snapshot instanceof Throwable) {
            Throwable t = (Throwable) snapshot;
            logger.severe("Could not take snapshot from service '" + serviceName + "', commit index: " + commitIndex, t);
            return;
        }
        RestoreSnapshotOp snapshotOp = new RestoreSnapshotOp(serviceName, state.name(), commitIndex, snapshot);
        LogEntry committedEntry = state.log().getLogEntry(commitIndex);
        LogEntry snapshotLogEntry = new LogEntry(committedEntry.term(), commitIndex, snapshotOp);
        state.log().setSnapshot(snapshotLogEntry);

        logger.info("Snapshot: "  + snapshotLogEntry + " is taken.");
    }

    public void installSnapshot(LogEntry snapshot) {
        int commitIndex = state.commitIndex();
        if (commitIndex > snapshot.index()) {
            logger.severe("Cannot install snapshot: " + snapshot + " since commit index: " + commitIndex + " is larger.");
            return;
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
    }

    public ICompletableFuture replicate(RaftOperation operation) {
        SimpleCompletableFuture resultFuture = new SimpleCompletableFuture(raftIntegration.getExecutor(), logger);
        executor.execute(new ReplicateTask(this, operation, resultFuture));
        return resultFuture;
    }

    public RaftEndpoint getLeader() {
        // read leader might be stale, since it's accessed without any synchronization
        return state.leader();
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

    private class HeartbeatTask implements StripedRunnable {

        @Override
        public void run() {
            if (state.role() == RaftRole.LEADER) {
                if (lastAppendEntriesTimestamp < Clock.currentTimeMillis() - heartbeatPeriodInMillis) {
                    broadcastAppendRequest();
                }

                scheduleNextRun();
            }
        }

        private void scheduleNextRun() {
            taskScheduler.schedule(new Runnable() {
                @Override
                public void run() {
                    executor.execute(new HeartbeatTask());
                }
            }, heartbeatPeriodInMillis, TimeUnit.MILLISECONDS);
        }

        @Override
        public int getKey() {
            return getStripeKey();
        }
    }

    private class LeaderFailureDetectionTask implements StripedRunnable {
        @Override
        public int getKey() {
            return getStripeKey();
        }

        @Override
        public void run() {
            try {
                RaftEndpoint leader = state.leader();
                if (leader == null) {
                    if (state.role() == RaftRole.FOLLOWER) {
                        logger.warning("We are FOLLOWER and there is no current leader. Will start new election round...");
                        runLeaderElectionTask();
                    }
                } else if (!raftIntegration.isReachable(leader)) {
                    logger.warning("Current leader " + leader + " is not reachable. Will start new election round...");
                    state.leader(null);
                    printMemberState();
                    runLeaderElectionTask();
                }
            } finally {
                scheduleLeaderFailureDetection();
            }
        }

        private void runLeaderElectionTask() {
            new LeaderElectionTask(RaftNode.this).run();
        }
    }

    private class SnapshotTask implements StripedRunnable {
        @Override
        public int getKey() {
            return getStripeKey();
        }

        @Override
        public void run() {
            if (state.role() == RaftRole.LEADER || state.role() == RaftRole.FOLLOWER) {
                takeSnapshotIfCommitIndexAdvanced();
            }
        }
    }
}
