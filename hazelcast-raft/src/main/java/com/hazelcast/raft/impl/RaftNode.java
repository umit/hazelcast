package com.hazelcast.raft.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.impl.async.AppendEntriesTask;
import com.hazelcast.raft.impl.async.LeaderElectionTask;
import com.hazelcast.raft.impl.async.ReplicateTask;
import com.hazelcast.raft.impl.async.RequestVoteTask;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.operation.HeartbeatOp;
import com.hazelcast.raft.impl.operation.RaftResponseHandler;
import com.hazelcast.raft.impl.util.AddressableExecutionCallback;
import com.hazelcast.raft.impl.util.ExecutionCallbackAdapter;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.raft.impl.util.StripeExecutorConveyor;
import com.hazelcast.spi.InternalCompletableFuture;
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

    public void handleRequestVote(VoteRequest voteRequest, RaftResponseHandler responseHandler) {
        executor.execute(new RequestVoteTask(this, voteRequest, responseHandler));
    }

    public void handleAppendEntries(AppendRequest appendRequest, RaftResponseHandler responseHandler) {
        executor.execute(new AppendEntriesTask(this, appendRequest, responseHandler));
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
        }, 5, 5, TimeUnit.SECONDS);
    }

    public void sendHeartbeat() {
        OperationService operationService = nodeEngine.getOperationService();
        AppendRequest appendRequest = new AppendRequest(state.term(), nodeEngine.getThisAddress(),
                state.lastLogTerm(), state.lastLogIndex(), state.commitIndex(), new LogEntry[0]);

        for (Address address : state.members()) {
            if (nodeEngine.getThisAddress().equals(address)) {
                continue;
            }
            operationService.send(new HeartbeatOp(state.name(), appendRequest), address);
        }
        lastAppendEntriesTimestamp = Clock.currentTimeMillis();
    }


    public void processLogs(int index) {
        // Reject logs we've applied already
        int lastApplied = state.lastApplied();
        if (index <= lastApplied) {
            logger.warning("Skipping application of old log: " + index);
            return;
        }

        // Apply all the preceding logs
        for (int idx = state.lastApplied() + 1; idx <= index; idx++) {
            LogEntry l = state.getLogEntry(idx);
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

    public <T> void registerCallback(InternalCompletableFuture<T> future, Address address,
            AddressableExecutionCallback<T> callback) {
        future.andThen(new ExecutionCallbackAdapter<T>(address, callback), new StripeExecutorConveyor(getStripeKey(), executor));
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

    private class HeartbeatTask implements StripedRunnable {

        @Override
        public void run() {
            if (lastAppendEntriesTimestamp < Clock.currentTimeMillis() - TimeUnit.SECONDS.toMillis(5)) {
                sendHeartbeat();
            }
        }

        @Override
        public int getKey() {
            return getStripeKey();
        }
    }
}
