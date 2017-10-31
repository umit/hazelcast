package com.hazelcast.raft.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendResponse;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.raft.impl.operation.AppendEntriesOp;
import com.hazelcast.raft.impl.operation.HeartbeatOp;
import com.hazelcast.raft.impl.operation.RaftResponseHandler;
import com.hazelcast.raft.impl.operation.RequestVoteOp;
import com.hazelcast.raft.impl.util.AddressableExecutionCallback;
import com.hazelcast.raft.impl.util.ExecutionCallbackAdapter;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.raft.impl.util.StripeExecutorConveyor;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.TaskScheduler;
import com.hazelcast.util.Clock;
import com.hazelcast.util.RandomPicker;
import com.hazelcast.util.executor.StripedExecutor;
import com.hazelcast.util.executor.StripedRunnable;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.impl.RaftService.SERVICE_NAME;
import static java.lang.Math.min;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftNode {
    
    private final RaftState state;
    private final Executor executor;
    private final NodeEngine nodeEngine;
    private final ILogger logger;
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
            executor.execute(new LeaderElectionTask());
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
        executor.execute(new RequestVoteTask(voteRequest, responseHandler));
    }

    public void handleAppendEntries(AppendRequest appendRequest, RaftResponseHandler responseHandler) {
        executor.execute(new AppendEntriesTask(appendRequest, responseHandler));
    }

    private abstract class StripedTask implements StripedRunnable {
        @Override
        public int getKey() {
            return getStripeKey();
        }
    }

    private int getStripeKey() {
        return state.name().hashCode();
    }

    private class LeaderElectionTask extends StripedTask {
        @Override
        public void run() {
            Address thisAddress = nodeEngine.getThisAddress();
            int timeout = RandomPicker.getInt(1000, 3000);
            if (state.votedFor() != null && !thisAddress.equals(state.votedFor())) {
                scheduleTimeout(timeout);
                return;
            }

            logger.warning("Leader election start");
            state.role(RaftRole.CANDIDATE);
            state.persistVote(state.incTerm(), thisAddress);

            if (state.members().size() == 1) {
                state.role(RaftRole.LEADER);
                logger.warning("We are the one! ");
                return;
            }


            OperationService operationService = nodeEngine.getOperationService();
            AddressableExecutionCallback<VoteResponse> callback = new LeaderElectionExecutionCallback(state.majority());
            for (Address address : state.members()) {
                if (address.equals(thisAddress)) {
                    continue;
                }

                RequestVoteOp op = new RequestVoteOp(state.name(), new VoteRequest(state.term(),
                        thisAddress, state.lastLogTerm(), state.lastLogIndex()));
                InternalCompletableFuture<VoteResponse> future =
                        operationService.createInvocationBuilder(SERVICE_NAME, op, address)
                        .setCallTimeout(timeout).invoke();
                registerCallback(future, address, callback);
            }


            scheduleTimeout(timeout);
        }

        private void scheduleTimeout(int timeout) {
            taskScheduler.schedule(new Runnable() {
                @Override
                public void run() {
                    executor.execute(new LeaderElectionTimeoutTask());
                }
            }, timeout, TimeUnit.MILLISECONDS);
        }

    }

    private class LeaderElectionTimeoutTask extends StripedTask {
        @Override
        public void run() {
            if (state.role() != RaftRole.CANDIDATE) {
                return;
            }
            logger.severe("Leader election timed out!");
            new LeaderElectionTask().run();
        }
    }

    private class LeaderElectionExecutionCallback implements AddressableExecutionCallback<VoteResponse> {
        final int majority;
        final Set<Address> voters = new HashSet<Address>();

        private LeaderElectionExecutionCallback(int majority) {
            this.majority = majority;
            voters.add(nodeEngine.getThisAddress());
        }

        @Override
        public void onResponse(Address voter, VoteResponse resp) {
            if (RaftRole.CANDIDATE != state.role()) {
                return;
            }

            if (resp.term > state.term()) {
                logger.warning("Newer term discovered, fallback to follower");
                state.role(RaftRole.FOLLOWER);
                state.term(resp.term);
                return;
            }

            if (resp.term < state.term()) {
                logger.warning("Obsolete vote response received: " + resp + ", current-term: " + state.term());
                return;
            }

            if (resp.granted && resp.term == state.term()) {
                if (voters.add(voter)) {
                    logger.warning("Vote granted from " + voter + " for term " + state.term()
                           + ", number of votes: " + voters.size() + ", majority: " + majority);
                }
            }

            if (voters.size() >= majority) {
                logger.severe("We are THE leader!");
                state.selfLeader();
                scheduleLeaderLoop();
            }
        }

        @Override
        public void onFailure(Address remote, Throwable t) {
        }
    }

    private void scheduleLeaderLoop() {
        executor.execute(new HeartbeatTask());
        taskScheduler.scheduleWithRepetition(new Runnable() {
            @Override
            public void run() {
                executor.execute(new HeartbeatTask());
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    private void sendHeartbeat() {
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

    private class RequestVoteTask extends StripedTask {
        private final VoteRequest req;
        private final RaftResponseHandler responseHandler;

        RequestVoteTask(VoteRequest req, RaftResponseHandler responseHandler) {
            this.req = req;
            this.responseHandler = responseHandler;
        }

        @Override
        public void run() {
            VoteResponse resp = new VoteResponse();
            try {
                if (state.leader() != null && !req.candidate.equals(state.leader())) {
                    logger.warning("Rejecting vote request from " + req.candidate + " since we have a leader " + state.leader());
                    rejectVoteResponse(resp);
                    return;
                }
                if (state.term() > req.term) {
                    logger.warning("Rejecting vote request from " + req.candidate
                            + " since our term is greater " + state.term() + " > " + req.term);
                    rejectVoteResponse(resp);
                    return;
                }

                if (state.term() < req.term) {
                    logger.warning("Demoting to FOLLOWER after vote request from " + req.candidate
                            + " since our term is lower " + state.term() + " < " + req.term);
                    state.role(RaftRole.FOLLOWER);
                    state.term(req.term);
                    resp.term = req.term;
                }

                if (state.lastVoteTerm() == req.term && state.votedFor() != null) {
                    logger.warning("Duplicate RequestVote for same term " + req.term + ", currently voted-for " + state.votedFor());
                    if (req.candidate.equals(state.votedFor())) {
                        logger.warning("Duplicate RequestVote from candidate " + req.candidate);
                        resp.granted = true;
                    }
                    return;
                }

                if (state.lastLogTerm() > req.lastLogTerm) {
                    logger.warning("Rejecting vote request from " + req.candidate + " since our last term is greater "
                            + state.lastLogTerm() + " > " + req.lastLogTerm);
                    return;
                }

                if (state.lastLogTerm() == req.lastLogTerm && state.lastLogIndex() > req.lastLogIndex) {
                    logger.warning("Rejecting vote request from " + req.candidate + " since our last index is greater "
                            + state.lastLogIndex() + " > " + req.lastLogIndex);
                    return;
                }

                logger.warning("Granted vote for " + req.candidate + ", term: " + req.term);
                state.persistVote(req.term, req.candidate);
                resp.granted = true;

            } finally {
                responseHandler.send(resp);
            }
        }
    }

    private void rejectVoteResponse(VoteResponse response) {
        response.granted = false;
        response.term = state.term();
    }

    private class AppendEntriesTask extends StripedTask {
        private final AppendRequest req;
        private final RaftResponseHandler responseHandler;

        AppendEntriesTask(AppendRequest req, RaftResponseHandler responseHandler) {
            this.req = req;
            this.responseHandler = responseHandler;
        }

        @Override
        public void run() {
            AppendResponse resp = new AppendResponse();
            resp.term = state.term();
            resp.lastLogIndex = state.lastLogIndex();

            try {
                if (req.term < state.term()) {
                    logger.warning("Older append entries received term: " + req.term + ", current-term: " + state.term());
                    return;
                }

                // Increase the term if we see a newer one, also transition to follower
                // if we ever get an appendEntries call
                if (req.term > state.term() || state.role() != RaftRole.FOLLOWER) {
                    // Ensure transition to follower
                    logger.warning("Transiting to FOLLOWER, term: " + req.term);
                    state.role(RaftRole.FOLLOWER);
                    state.term(req.term);
                    resp.term = req.term;
                }

                if (!req.leader.equals(state.leader())) {
                    logger.severe("Setting leader " + req.leader);
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
                            logger.warning("Failed to get previous log " + req.prevLogIndex + ", last-index: " + lastIndex);
//                            resp.NoRetryBackoff = true
                            return;
                        }
                        prevLogTerm = prevLog.term;
                    }

                    if (req.prevLogTerm != prevLogTerm) {
                        logger.warning("Previous log term mis-match: ours: " + prevLogTerm + ", remote: " + req.prevLogTerm);
//                        resp.NoRetryBackoff = true
                        return;
                    }
                }

                // Process any new entries
                if (req.entries.length > 0 ) {
                    // Delete any conflicting entries, skip any duplicates
                    int lastLogIndex = state.lastLogIndex();

                    LogEntry[] newEntries = null;
                    for (int i = 0; i < req.entries.length; i++) {
                        LogEntry entry = req.entries[i];

                        if (entry.index > lastLogIndex) {
                            newEntries = Arrays.copyOfRange(req.entries, i, req.entries.length);
                            break;
                        }

                        LogEntry storeEntry = state.getLogEntry(entry.index);
                        if (storeEntry == null) {
                            logger.warning("Failed to get log entry: " + entry.index);
                            return;
                        }

                        if (entry.term != storeEntry.term) {
                            logger.warning("Clearing log suffix from " + entry.index + " to " + lastLogIndex);
                            try {
                                state.deleteLogAfter(entry.index);
                            } catch (Exception e) {
                                logger.severe("Failed to clear log from " + entry.index + " to " + lastLogIndex, e);
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
                            logger.severe("Failed to append to logs", e);
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
                    int idx = min(req.leaderCommitIndex, state.lastLogIndex());
                    state.commitIndex(idx);
//                    if r.configurations.latestIndex <= idx {
//                        r.configurations.committed = r.configurations.latest
//                        r.configurations.committedIndex = r.configurations.latestIndex
//                    }
                    processLogs(idx);
                }

                // Everything went well, set success
                resp.success = true;
            } finally {
                responseHandler.send(resp);
            }
        }
    }

    private void processLogs(int index) {
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

    private class HeartbeatTask extends StripedTask {

        @Override
        public void run() {
            if (lastAppendEntriesTimestamp < Clock.currentTimeMillis() - TimeUnit.SECONDS.toMillis(5)) {
                sendHeartbeat();
            }
        }
    }

    public Future replicate(Object value) {
        SimpleCompletableFuture resultFuture = new SimpleCompletableFuture(nodeEngine);
        executor.execute(new ReplicateTask(value, resultFuture));
        return resultFuture;
    }

    private class ReplicateTask extends StripedTask {
        private final Object value;
        private final SimpleCompletableFuture resultFuture;

        public ReplicateTask(Object value, SimpleCompletableFuture resultFuture) {
            this.value = value;
            this.resultFuture = resultFuture;
        }

        @Override
        public void run() {

            // TODO: debug
            if (state.role() != RaftRole.LEADER) {
                return;
            }

            logger.info("Replicating: " + value);

            assert state.role() == RaftRole.LEADER;

            int lastLogIndex = state.lastLogIndex();
            int lastLogTerm = state.lastLogTerm();

            LogEntry entry = new LogEntry(state.term(), lastLogIndex + 1, value);
            state.storeLogs(entry);

            AppendRequest request = new AppendRequest(state.term(), nodeEngine.getThisAddress(),
                    lastLogTerm, lastLogIndex, state.commitIndex(), new LogEntry[]{entry});

            AddressableExecutionCallback<AppendResponse> callback = new AppendEntriesExecutionCallback(request, state.majority(), resultFuture);

            OperationService operationService = nodeEngine.getOperationService();
            for (Address address : state.members()) {
                if (nodeEngine.getThisAddress().equals(address)) {
                    continue;
                }

                AppendEntriesOp op = new AppendEntriesOp(state.name(), request);
                InternalCompletableFuture<AppendResponse> future = operationService.invokeOnTarget(SERVICE_NAME, op, address);
                registerCallback(future, address, callback);
            }
        }
    }

    private class AppendEntriesExecutionCallback implements AddressableExecutionCallback<AppendResponse> {
        private final AppendRequest req;
        private final int majority;
        private final Set<Address> addresses = new HashSet<Address>();
        private final Set<Address> erroneousAddresses = new HashSet<Address>();
        private final SimpleCompletableFuture resultFuture;

        AppendEntriesExecutionCallback(AppendRequest request, int majority, SimpleCompletableFuture resultFuture) {
            this.req = request;
            this.majority = majority;
            this.resultFuture = resultFuture;
            addresses.add(nodeEngine.getThisAddress());
        }

        @Override
        public void onResponse(Address follower, AppendResponse resp) {
            // Check for a newer term, stop running
            if (resp.term > req.term) {
//                r.handleStaleTerm(s)
                return;
            }

            // Abort pipeline if not successful
            if (!resp.success) {
                logger.severe("Failure response " + resp);
                // TODO: handle?
                return;
            }

            assert req.entries.length > 0;

            if (addresses.add(follower)) {
                logger.warning("Success response " + resp);

                // Update our replication state
                LogEntry last = req.entries[req.entries.length - 1];
                LeaderState leaderState = state.getLeaderState();
                leaderState.nextIndex(follower, last.index + 1);
                leaderState.matchIndex(follower, last.index);
            }

            if (addresses.size() >= majority) {
                LogEntry last = req.entries[req.entries.length - 1];
                state.commitIndex(last.index);
                sendHeartbeat();
                processLogs(state.commitIndex());
                Object result = req.entries[0].data;
                resultFuture.setResult(result);
                return;
            }

            if (addresses.size() + erroneousAddresses.size() >= state.members().size()) {

            }
        }

        @Override
        public void onFailure(Address remote, Throwable t) {
            erroneousAddresses.add(remote);
        }
    }

    private <T> void registerCallback(InternalCompletableFuture<T> future, Address address, AddressableExecutionCallback<T> callback) {
        future.andThen(new ExecutionCallbackAdapter<T>(address, callback), new StripeExecutorConveyor(getStripeKey(), executor));
    }
}
