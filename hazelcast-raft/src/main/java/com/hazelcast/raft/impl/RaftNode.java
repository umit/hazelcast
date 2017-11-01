package com.hazelcast.raft.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.impl.async.AppendRequestTask;
import com.hazelcast.raft.impl.async.AppendResponseTask;
import com.hazelcast.raft.impl.async.LeaderElectionTask;
import com.hazelcast.raft.impl.async.ReplicateTask;
import com.hazelcast.raft.impl.async.VoteRequestTask;
import com.hazelcast.raft.impl.async.VoteResponseTask;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendResponse;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.raft.impl.operation.AppendRequestOp;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.TaskScheduler;
import com.hazelcast.util.Clock;
import com.hazelcast.util.executor.StripedExecutor;
import com.hazelcast.util.executor.StripedRunnable;

import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftNode {

    private static final int HEARTBEAT_PERIOD = 5;

    public final ILogger logger;
    private final RaftState state;
    private final Executor executor;
    private final NodeEngine nodeEngine;
    private final TaskScheduler taskScheduler;

    private long lastAppendEntriesTimestamp;

    public RaftNode(String name, Collection<Address> addresses, NodeEngine nodeEngine, StripedExecutor executor) {
        this.nodeEngine = nodeEngine;
        this.executor = executor;
        this.state = new RaftState(name, nodeEngine.getThisAddress(), addresses);
        this.taskScheduler = nodeEngine.getExecutionService().getGlobalTaskScheduler();
        this.logger = nodeEngine.getLogger(getClass().getSimpleName() + "[" + name + "]");
    }

    public void start() {
        if (nodeEngine.getClusterService().isJoined()) {
            logger.warning("Starting raft group...");
            executor.execute(new LeaderElectionTask(this));
        } else {
            scheduleStart();
        }
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
        // TODO: increment commit index with an empty commit
        executor.execute(new HeartbeatTask());
        taskScheduler.scheduleWithRepetition(new Runnable() {
            @Override
            public void run() {
                executor.execute(new HeartbeatTask());
            }
        }, HEARTBEAT_PERIOD, HEARTBEAT_PERIOD, TimeUnit.SECONDS);
    }

    public void sendHeartbeat() {
        for (Address follower : state.members()) {
            if (nodeEngine.getThisAddress().equals(follower)) {
                continue;
            }
            sendHeartbeat(follower);
        }
        lastAppendEntriesTimestamp = Clock.currentTimeMillis();
    }

    public void sendHeartbeat(Address follower) {
        OperationService operationService = nodeEngine.getOperationService();
        RaftLog raftLog = state.log();
        LeaderState leaderState = state.leaderState();

        int index = leaderState.getNextIndex(follower);

        LogEntry prevEntry;
        LogEntry[] entries;
        // TODO: define a max batch size
        if (index > 1) {
            prevEntry = raftLog.getEntry(index - 1);
            entries = raftLog.getEntriesBetween(index, raftLog.lastLogIndex());
        } else if (index == 1 && raftLog.lastLogIndex() > 0) {
            prevEntry = new LogEntry();
            entries = raftLog.getEntriesBetween(index, raftLog.lastLogIndex());
        } else {
            prevEntry = new LogEntry();
            entries = new LogEntry[0];
        }

        AppendRequest appendRequest = new AppendRequest(state.term(), nodeEngine.getThisAddress(),
                prevEntry.term(), prevEntry.index(), state.commitIndex(), entries);

        logger.warning("Sending heartbeat " + appendRequest + " to " + follower + " with next-index: " + index);

        operationService.send(new AppendRequestOp(state.name(), appendRequest), follower);
    }

    // If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
    public void processLogs(int index) {
        // Reject logs we've applied already
        int lastApplied = state.lastApplied();
        if (index <= lastApplied) {
            logger.warning("Skipping application of old log: " + index + ", lastApplied: " + lastApplied);
            return;
        }

        // Apply all the preceding logs
        RaftLog raftLog = state.log();
        for (int idx = state.lastApplied() + 1; idx <= index; idx++) {
            LogEntry l = raftLog.getEntry(idx);
            if (l == null) {
                logger.severe("Failed to get log at " +  idx);
                throw new AssertionError("Failed to get log at " +  idx);
            }
            processLog(l);

            // Update the lastApplied index and term
            state.lastApplied(idx);
        }
    }

    private void processLog(LogEntry entry) {
        logger.severe("Processing log " + entry);
    }

    public Future replicate(Object value) {
        SimpleCompletableFuture resultFuture = new SimpleCompletableFuture(nodeEngine);
        executor.execute(new ReplicateTask(this, value, resultFuture));
        return resultFuture;
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
        executor.execute(new VoteRequestTask(this, request));
    }

    public void handleVoteResponse(VoteResponse response) {
        executor.execute(new VoteResponseTask(this, response));
    }

    public void handleAppendRequest(AppendRequest request) {
        executor.execute(new AppendRequestTask(this, request));
    }

    public void handleAppendResponse(AppendResponse response) {
        executor.execute(new AppendResponseTask(this, response));
    }

    private class HeartbeatTask implements StripedRunnable {

        @Override
        public void run() {
            if (lastAppendEntriesTimestamp < Clock.currentTimeMillis() - TimeUnit.SECONDS.toMillis(HEARTBEAT_PERIOD)) {
                sendHeartbeat();
            }
        }

        @Override
        public int getKey() {
            return getStripeKey();
        }
    }
}
