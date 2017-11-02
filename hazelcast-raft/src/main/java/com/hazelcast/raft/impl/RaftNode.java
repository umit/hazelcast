package com.hazelcast.raft.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.impl.async.AppendRequestHandlerTask;
import com.hazelcast.raft.impl.async.AppendResponseHandlerTask;
import com.hazelcast.raft.impl.async.LeaderElectionTask;
import com.hazelcast.raft.impl.async.ReplicateTask;
import com.hazelcast.raft.impl.async.VoteRequestHandlerTask;
import com.hazelcast.raft.impl.async.VoteResponseHandlerTask;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendResponse;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.raft.impl.operation.AppendRequestOp;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.TaskScheduler;
import com.hazelcast.util.Clock;
import com.hazelcast.util.RandomPicker;
import com.hazelcast.util.collection.Long2ObjectHashMap;
import com.hazelcast.util.executor.StripedExecutor;
import com.hazelcast.util.executor.StripedRunnable;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftNode {

    private static final int HEARTBEAT_PERIOD = 5;

    private final ILogger logger;
    private final RaftState state;
    private final Executor executor;
    private final NodeEngine nodeEngine;
    private final TaskScheduler taskScheduler;

    private final Long2ObjectHashMap<SimpleCompletableFuture> futures = new Long2ObjectHashMap<SimpleCompletableFuture>();
    private long lastAppendEntriesTimestamp;

    public RaftNode(String name, Collection<Address> addresses, NodeEngine nodeEngine, StripedExecutor executor) {
        this.nodeEngine = nodeEngine;
        this.executor = executor;
        this.state = new RaftState(name, nodeEngine.getThisAddress(), addresses);
        this.taskScheduler = nodeEngine.getExecutionService().getGlobalTaskScheduler();
        this.logger = getLogger(getClass());
    }

    public ILogger getLogger(Class clazz) {
        String name = state.name();
        return nodeEngine.getLogger(clazz.getSimpleName() + "[" + name + "]");
    }

    public Address getThisAddress() {
        return nodeEngine.getThisAddress();
    }

    public void send(Operation operation, Address target) {
        nodeEngine.getOperationService().send(operation, target);
    }

    public void start() {
        if (nodeEngine.getClusterService().isJoined()) {
            logger.warning("Starting raft group...");
            executor.execute(new LeaderElectionTask(this));
        } else {
            scheduleStart();
        }

        scheduleLeaderFailureDetection();
    }

    private void scheduleLeaderFailureDetection() {
        long delay = RandomPicker.getInt(1000, 1500);
        taskScheduler.schedule(new Runnable() {
            @Override
            public void run() {
                executor.execute(new LeaderFailureDetectionTask());
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    private void scheduleStart() {
        taskScheduler.schedule(new Runnable() {
            @Override
            public void run() {
                start();
            }
        }, 500, TimeUnit.MILLISECONDS);
    }

    public int getStripeKey() {
        return state.name().hashCode();
    }

    public void scheduleLeaderLoop() {
        executor.execute(new HeartbeatTask());
        taskScheduler.scheduleWithRepetition(new Runnable() {
            @Override
            public void run() {
                executor.execute(new HeartbeatTask());
            }
        }, HEARTBEAT_PERIOD, HEARTBEAT_PERIOD, TimeUnit.SECONDS);
    }

    public void broadcastAppendRequest() {
        for (Address follower : state.remoteMembers()) {
            sendAppendRequest(follower);
        }
        lastAppendEntriesTimestamp = Clock.currentTimeMillis();
    }

    public void sendAppendRequest(Address follower) {
        RaftLog raftLog = state.log();
        LeaderState leaderState = state.leaderState();

        int nextIndex = leaderState.getNextIndex(follower);

        LogEntry prevEntry;
        LogEntry[] entries;
        // TODO: define a max batch size
        if (nextIndex > 1) {
            prevEntry = raftLog.getEntry(nextIndex - 1);
            entries = raftLog.getEntriesBetween(nextIndex, raftLog.lastLogIndex());
        } else if (nextIndex == 1 && raftLog.lastLogIndex() > 0) {
            prevEntry = new LogEntry();
            entries = raftLog.getEntriesBetween(nextIndex, raftLog.lastLogIndex());
        } else {
            prevEntry = new LogEntry();
            entries = new LogEntry[0];
        }

        AppendRequest appendRequest = new AppendRequest(getThisAddress(), state.term(), prevEntry.term(), prevEntry.index(),
                state.commitIndex(), entries);

        logger.warning("Sending " + appendRequest + " to " + follower + " with next index: " + nextIndex);

        send(new AppendRequestOp(state.name(), appendRequest), follower);
    }

    // If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
    public void processLogs() {
        // Reject logs we've applied already
        int commitIndex = state.commitIndex();
        int lastApplied = state.lastApplied();
        if (commitIndex <= lastApplied) {
            logger.warning("Skipping application of stale log commit index: " + commitIndex + ", lastApplied: " + lastApplied);
            return;
        }

        // Apply all the preceding logs
        RaftLog raftLog = state.log();
        for (int idx = state.lastApplied() + 1; idx <= commitIndex; idx++) {
            LogEntry entry = raftLog.getEntry(idx);
            if (entry == null) {
                logger.severe("Failed to get log at " +  idx);
                throw new AssertionError("Failed to get log at " +  idx);
            }
            processLog(entry);

            // Update the lastApplied index and term
            state.lastApplied(idx);
        }
    }

    private void processLog(LogEntry entry) {
        logger.severe("Processing log " + entry);
        SimpleCompletableFuture future = futures.remove(entry.index());
        if (future != null) {
            future.setResult(entry.data());
        }
    }

    public RaftState state() {
        return state;
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    public TaskScheduler taskScheduler() {
        return taskScheduler;
    }

    public Executor executor() {
        return executor;
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

    public void handleAppendResponse(AppendResponse response) {
        executor.execute(new AppendResponseHandlerTask(this, response));
    }

    public void registerFuture(int logIndex, SimpleCompletableFuture future) {
        SimpleCompletableFuture f = futures.put(logIndex, future);
        assert f == null : "Index : " + logIndex + " -> " + f;
    }

    public void invalidateFuturesFrom(int entryIndex) {
        Iterator<Map.Entry<Long, SimpleCompletableFuture>> iterator = futures.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, SimpleCompletableFuture> entry = iterator.next();
            long index = entry.getKey();
            if (index >= entryIndex) {
                logger.severe("Truncating log entry at index: " + index);
                iterator.remove();
                entry.getValue().setResult(new IllegalStateException("Truncated: " + index));
            }
        }
    }

    public Future replicate(Object value) {
        SimpleCompletableFuture resultFuture = new SimpleCompletableFuture(nodeEngine);
        executor.execute(new ReplicateTask(this, value, resultFuture));
        return resultFuture;
    }


    private class HeartbeatTask implements StripedRunnable {

        @Override
        public void run() {
            if (lastAppendEntriesTimestamp < Clock.currentTimeMillis() - TimeUnit.SECONDS.toMillis(HEARTBEAT_PERIOD)) {
                broadcastAppendRequest();
            }
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
                Address leader = state.leader();
                if (leader != null && nodeEngine.getClusterService().getMember(leader) == null) {
                    state.leader(null);
                    new LeaderElectionTask(RaftNode.this).run();
                }
            } finally {
                scheduleLeaderFailureDetection();
            }
        }
    }
}
