package com.hazelcast.raft;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.operation.RaftResponseHandler;
import com.hazelcast.raft.operation.RequestVoteOp;
import com.hazelcast.raft.util.StripeExecutorConveyor;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.TaskScheduler;
import com.hazelcast.util.RandomPicker;
import com.hazelcast.util.executor.StripedExecutor;
import com.hazelcast.util.executor.StripedRunnable;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * TODO: Javadoc Pending...
 *
 * @author mdogan 30.10.2017
 */
public class RaftNode {
    
    private final RaftContext context;
    private final Executor executor;
    private final NodeEngine nodeEngine;
    private final ILogger logger;
    private final TaskScheduler taskScheduler;

    public RaftNode(String name, Collection<Address> addresses, NodeEngine nodeEngine, StripedExecutor executor) {
        this.nodeEngine = nodeEngine;
        this.executor = executor;
        this.context = new RaftContext(name, addresses);
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

    private class LeaderElectionTask implements StripedRunnable {
        @Override
        public int getKey() {
            return getStripeKey();
        }

        @Override
        public void run() {
            logger.warning("Leader election start");
            context.role(RaftRole.CANDIDATE);
            Address thisAddress = nodeEngine.getThisAddress();
            context.persistVote(context.incTerm(), thisAddress);

            if (context.members().size() == 1) {
                context.role(RaftRole.LEADER);
                logger.warning("We are the one! ");
                return;
            }

            OperationService operationService = nodeEngine.getOperationService();
            Collection<InternalCompletableFuture> futures = new LinkedList<InternalCompletableFuture>();
            for (Address address : context.members()) {
                if (address.equals(thisAddress)) {
                    continue;
                }

                RequestVoteOp op = new RequestVoteOp(context.name(), new VoteRequest(context.term(),
                        thisAddress, 0, 0));
                InternalCompletableFuture<Object> future =
                        operationService.invokeOnTarget(RaftService.SERVICE_NAME, op, address);
                futures.add(future);
            }

            int timeout = RandomPicker.getInt(1500, 3000);

            ExecutionCallback callback = new LeaderElectionCallback();

            for (InternalCompletableFuture future : futures) {
                future.andThen(callback, new StripeExecutorConveyor(getStripeKey(), executor));
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

    private class LeaderElectionTimeoutTask implements StripedRunnable {
        @Override
        public int getKey() {
            return getStripeKey();
        }

        @Override
        public void run() {
            if (context.role() != RaftRole.CANDIDATE) {
                return;
            }
            logger.severe("Leader election timed out!");
            new LeaderElectionTask().run();
        }
    }

    private class LeaderElectionCallback implements ExecutionCallback<VoteResponse> {
        int majority;
        Set<Address> voters = new HashSet<Address>();

        @Override
        public void onResponse(VoteResponse resp) {
            if (RaftRole.CANDIDATE != context.role()) {
                return;
            }

            if (resp.term > context.term()) {
                logger.warning("Newer term discovered, fallback to follower");
                context.role(RaftRole.FOLLOWER);
                context.term(resp.term);
                return;
            }

            if (resp.term < context.term()) {
                logger.warning("Obsolete vote response received: " + resp);
                return;
            }

            if (resp.granted && resp.term == context.term()) {
                if (voters.add(resp.voter)) {
                    logger.warning("Vote granted from " + resp.voter + " for term " + context.term()
                           + ", number of votes: " + voters.size());
                }
            }

            if (voters.size() >= majority) {
                logger.severe("We are THE leader!");
                context.role(RaftRole.LEADER);
                context.leader(nodeEngine.getThisAddress());
            }
        }

        @Override
        public void onFailure(Throwable t) {
        }
    }

    private int getStripeKey() {
        return context.name().hashCode();
    }

    private class RequestVoteTask implements StripedRunnable {
        private final VoteRequest req;
        private final RaftResponseHandler responseHandler;

        public RequestVoteTask(VoteRequest req, RaftResponseHandler responseHandler) {
            this.req = req;
            this.responseHandler = responseHandler;
        }

        @Override
        public int getKey() {
            return getStripeKey();
        }

        @Override
        public void run() {
            VoteResponse resp = new VoteResponse();
            resp.voter = nodeEngine.getThisAddress();
            try {
                if (context.leader() != null && !req.candidate.equals(context.leader())) {
                    logger.warning("Rejecting vote request from " + req.candidate + " since we have a leader " + context.leader());
                    rejectVoteResponse(resp);
                    return;
                }
                if (context.term() > req.term) {
                    logger.warning("Rejecting vote request from " + req.candidate
                            + " since our term is greater " + context.term() + " > " + req.term);
                    rejectVoteResponse(resp);
                    return;
                }

                if (context.term() < req.term) {
                    logger.warning("Demoting to FOLLOWER after vote request from " + req.candidate
                            + " since our term is lower " + context.term() + " < " + req.term);
                    context.role(RaftRole.FOLLOWER);
                    context.term(req.term);
                    resp.term = req.term;
                }

                if (context.lastVoteTerm() == req.term && context.votedFor() != null) {
                    logger.warning("Duplicate RequestVote for same term " + req.term + ", currently voted-for " + context.votedFor());
                    if (req.candidate.equals(context.votedFor())) {
                        logger.warning("Duplicate RequestVote from candidate " + req.candidate);
                        resp.granted = true;
                    }
                    return;
                }

                if (context.lastLogTerm() > req.lastLogTerm) {
                    logger.warning("Rejecting vote request from " + req.candidate + " since our last term is greater "
                            + context.lastLogTerm() + " > " + req.lastLogTerm);
                    return;
                }

                if (context.lastLogTerm() == req.lastLogTerm && context.lastLogIndex() > req.lastLogIndex) {
                    logger.warning("Rejecting vote request from " + req.candidate + " since our last index is greater "
                            + context.lastLogIndex() + " > " + req.lastLogIndex);
                    return;
                }

                logger.warning("Granted vote for " + req.candidate + ", term: " + req.term);
                context.persistVote(req.term, req.candidate);
                resp.granted = true;

            } finally {
                responseHandler.send(resp);
            }
        }
    }

    private VoteResponse rejectVoteResponse(VoteResponse response) {
        response.granted = false;
        response.term = context.term();
        response.voter = nodeEngine.getThisAddress();
        return response;
    }
}
