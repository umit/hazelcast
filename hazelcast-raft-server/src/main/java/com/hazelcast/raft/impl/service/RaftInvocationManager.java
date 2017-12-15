package com.hazelcast.raft.impl.service;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.QueryPolicy;
import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.exception.LeaderDemotedException;
import com.hazelcast.raft.exception.NotLeaderException;
import com.hazelcast.raft.exception.RaftException;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.service.exception.CannotCreateRaftGroupException;
import com.hazelcast.raft.impl.service.operation.metadata.CreateRaftGroupOp;
import com.hazelcast.raft.impl.service.operation.metadata.GetActiveEndpointsOp;
import com.hazelcast.raft.impl.service.operation.metadata.TriggerDestroyRaftGroupOp;
import com.hazelcast.raft.impl.service.proxy.DefaultRaftQueryOperation;
import com.hazelcast.raft.impl.service.proxy.DefaultRaftReplicateOperation;
import com.hazelcast.raft.impl.service.proxy.RaftQueryOperation;
import com.hazelcast.raft.impl.service.proxy.RaftReplicateOperation;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.AbstractCompletableFuture;
import com.hazelcast.util.function.Supplier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.impl.service.RaftService.METADATA_GROUP_ID;
import static com.hazelcast.spi.ExecutionService.ASYNC_EXECUTOR;

public class RaftInvocationManager {

    private final NodeEngine nodeEngine;
    private final RaftService raftService;
    private final ILogger logger;
    private final ConcurrentMap<RaftGroupId, RaftEndpoint> knownLeaders = new ConcurrentHashMap<RaftGroupId, RaftEndpoint>();
    private final RaftEndpoint[] allEndpoints;
    private final boolean failOnIndeterminateOperationState;

    RaftInvocationManager(NodeEngine nodeEngine, RaftService raftService, RaftConfig config) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.raftService = raftService;
        this.allEndpoints = raftService.getMetadataManager().getAllEndpoints().toArray(new RaftEndpoint[0]);
        this.failOnIndeterminateOperationState = config.isFailOnIndeterminateOperationState();
    }

    public void init() {
    }

    public void reset() {
        knownLeaders.clear();
    }


    public <T> ICompletableFuture<T> query(Supplier<RaftQueryOperation> operationSupplier, QueryPolicy queryPolicy) {
        RaftQueryInvocationFuture<T> invocationFuture = new RaftQueryInvocationFuture<T>(operationSupplier, queryPolicy);
        invocationFuture.invoke();
        return invocationFuture;
    }

    public <T> ICompletableFuture<T> queryOnLocal(RaftQueryOperation operation, QueryPolicy queryPolicy) {
        return nodeEngine.getOperationService().invokeOnTarget(RaftService.SERVICE_NAME,
                operation.setQueryPolicy(queryPolicy), nodeEngine.getThisAddress());
    }

    public ICompletableFuture<RaftGroupId> createRaftGroupAsync(final String serviceName, final String raftName,
                                                                final int nodeCount) {
        Executor executor = nodeEngine.getExecutionService().getExecutor(ASYNC_EXECUTOR);
        ILogger logger = nodeEngine.getLogger(getClass());
        SimpleCompletableFuture<RaftGroupId> resultFuture = new SimpleCompletableFuture<RaftGroupId>(executor, logger);
        invokeGetEndpointsToCreateRaftGroup(serviceName, raftName, nodeCount, resultFuture);
        return resultFuture;
    }

    public RaftGroupId createRaftGroup(final String serviceName, final String raftName, final int nodeCount)
            throws ExecutionException, InterruptedException {
        return createRaftGroupAsync(serviceName, raftName, nodeCount).get();
    }

    private void invokeGetEndpointsToCreateRaftGroup(final String serviceName, final String raftName, final int nodeCount,
                                                     final SimpleCompletableFuture<RaftGroupId> resultFuture) {
        ICompletableFuture<List<RaftEndpoint>> f = query(new Supplier<RaftQueryOperation>() {
            @Override
            public RaftQueryOperation get() {
                return new DefaultRaftQueryOperation(METADATA_GROUP_ID, new GetActiveEndpointsOp());
            }
        }, QueryPolicy.LEADER_LOCAL);

        f.andThen(new ExecutionCallback<List<RaftEndpoint>>() {
            @Override
            public void onResponse(List<RaftEndpoint> endpoints) {
                endpoints = new ArrayList<RaftEndpoint>(endpoints);

                if (endpoints.size() < nodeCount) {
                    Exception result = new IllegalArgumentException("There are not enough active endpoints to create raft group "
                            + raftName + ". Active endpoints: " + endpoints.size() + ", Requested count: " + nodeCount);
                    resultFuture.setResult(result);
                    return;
                }

                Collections.shuffle(endpoints);
                endpoints = endpoints.subList(0, nodeCount);
                invokeCreateRaftGroup(serviceName, raftName, nodeCount, endpoints, resultFuture);
            }

            @Override
            public void onFailure(Throwable t) {
                resultFuture.setResult(t);
            }
        });
    }

    private void invokeCreateRaftGroup(final String serviceName, final String raftName, final int nodeCount,
                                       final List<RaftEndpoint> endpoints,
                                       final SimpleCompletableFuture<RaftGroupId> resultFuture) {
        ICompletableFuture<RaftGroupId> f = invoke(new Supplier<RaftReplicateOperation>() {
            @Override
            public RaftReplicateOperation get() {
                return new DefaultRaftReplicateOperation(METADATA_GROUP_ID, new CreateRaftGroupOp(serviceName, raftName, endpoints));
            }
        });

        f.andThen(new ExecutionCallback<RaftGroupId>() {
            @Override
            public void onResponse(RaftGroupId groupId) {
                resultFuture.setResult(groupId);
            }

            @Override
            public void onFailure(Throwable t) {
                if (t.getCause() instanceof CannotCreateRaftGroupException) {
                    logger.fine("Could not create raft group: " + raftName + " with endpoints: " + endpoints,
                            t.getCause());
                    invokeGetEndpointsToCreateRaftGroup(serviceName, raftName, nodeCount, resultFuture);
                    return;
                }

                resultFuture.setResult(t);
            }
        });
    }

    public ICompletableFuture<RaftGroupId> triggerDestroyRaftGroupAsync(final RaftGroupId groupId) {
        return invoke(new Supplier<RaftReplicateOperation>() {
            @Override
            public RaftReplicateOperation get() {
                return new DefaultRaftReplicateOperation(METADATA_GROUP_ID, new TriggerDestroyRaftGroupOp(groupId));
            }
        });
    }

    public void triggerDestroyRaftGroup(RaftGroupId groupId) throws ExecutionException, InterruptedException {
        triggerDestroyRaftGroupAsync(groupId).get();
    }

    public <T> ICompletableFuture<T> invoke(Supplier<RaftReplicateOperation> operationSupplier) {
        RaftInvocationFuture<T> invocationFuture = new RaftInvocationFuture<T>(operationSupplier);
        invocationFuture.invoke();
        return invocationFuture;
    }

    private void resetKnownLeader(RaftGroupId groupId) {
        logger.fine("Resetting known leader for raft: " + groupId);
        knownLeaders.remove(groupId);
    }

    private void setKnownLeader(RaftGroupId groupId, RaftEndpoint leader) {
        logger.fine("Setting known leader for raft: " + groupId + " to " + leader);
        knownLeaders.put(groupId, leader);
    }

    private RaftEndpoint getKnownLeader(RaftGroupId groupId) {
        return knownLeaders.get(groupId);
    }

    private void updateKnownLeaderOnFailure(RaftGroupId groupId, Throwable cause) {
        if (cause instanceof RaftException) {
            RaftException e = (RaftException) cause;
            RaftEndpoint leader = e.getLeader();
            if (leader != null) {
                setKnownLeader(groupId, leader);
            } else {
                resetKnownLeader(groupId);
            }
        } else {
            resetKnownLeader(groupId);
        }
    }

    private abstract class AbstractRaftInvocationFuture<T, O extends Operation>
            extends AbstractCompletableFuture<T> implements ExecutionCallback<T> {

        private final Supplier<O> operationSupplier;
        volatile RaftGroupId groupId;
        private volatile RaftEndpoint[] endpoints;
        private volatile int endPointIndex;

        AbstractRaftInvocationFuture(Supplier<O> operationSupplier) {
            super(nodeEngine, RaftInvocationManager.this.logger);
            this.operationSupplier = operationSupplier;
        }

        @Override
        public void onResponse(T response) {
            setResult(response);
        }

        @Override
        public void onFailure(Throwable cause) {
            logger.warning(cause);
            if (isRetryable(cause)) {
                updateKnownLeaderOnFailure(groupId, cause);
                try {
                    scheduleRetry();
                } catch (Throwable e) {
                    logger.warning(e);
                    setResult(e);
                }
            } else {
                setResult(cause);
            }
        }

        boolean isRetryable(Throwable cause) {
            return cause instanceof NotLeaderException
                    || cause instanceof LeaderDemotedException
                    || cause instanceof MemberLeftException
                    || cause instanceof CallerNotMemberException
                    || cause instanceof TargetNotMemberException;
        }

        final void scheduleRetry() {
            // TODO: needs a back-off strategy
            nodeEngine.getExecutionService().schedule(new Runnable() {
                @Override
                public void run() {
                    try {
                        invoke();
                    } catch (Throwable e) {
                        logger.severe(e);
                        setResult(e);
                    }
                }
            }, 250, TimeUnit.MILLISECONDS);
        }

        final void invoke() {
            O op = createOp();
            groupId = getGroupId(op);
            RaftEndpoint target = getTarget();
            if (target == null) {
                scheduleRetry();
                return;
            }

            InternalCompletableFuture<T> future = nodeEngine.getOperationService()
                    .invokeOnTarget(RaftService.SERVICE_NAME, op, target.getAddress());
            afterInvoke(target);
            future.andThen(this);
        }

        O createOp() {
            return operationSupplier.get();
        }

        abstract RaftGroupId getGroupId(O op);

        void afterInvoke(RaftEndpoint target) {
        }

        private RaftEndpoint getTarget() {
            RaftEndpoint target = getKnownTarget();
            if (target != null) {
                return target;
            }

            if (endpoints == null || endPointIndex == endpoints.length) {
                RaftGroupInfo raftGroupInfo = raftService.getRaftGroupInfo(groupId);
                endpoints = raftGroupInfo != null ? raftGroupInfo.membersArray() : allEndpoints;
                endPointIndex = 0;
            }

            return endpoints != null ? endpoints[endPointIndex++] : null;
        }

        RaftEndpoint getKnownTarget() {
            return getKnownLeader(groupId);
        }
    }

    private class RaftInvocationFuture<T> extends AbstractRaftInvocationFuture<T, RaftReplicateOperation> {

        private volatile RaftEndpoint lastInvocationEndpoint;

        RaftInvocationFuture(Supplier<RaftReplicateOperation> operationSupplier) {
            super(operationSupplier);
        }

        @Override
        public void onResponse(T response) {
            setKnownLeader(groupId, lastInvocationEndpoint);
            setResult(response);
        }

        @Override
        boolean isRetryable(Throwable cause) {
            if (failOnIndeterminateOperationState && cause instanceof MemberLeftException) {
                return false;
            }
            return super.isRetryable(cause);
        }

        @Override
        RaftGroupId getGroupId(RaftReplicateOperation op) {
            return op.getRaftGroupId();
        }

        @Override
        void afterInvoke(RaftEndpoint target) {
            lastInvocationEndpoint = target;
        }
    }

    private class RaftQueryInvocationFuture<T> extends AbstractRaftInvocationFuture<T, RaftQueryOperation> {

        private final QueryPolicy queryPolicy;

        RaftQueryInvocationFuture(Supplier<RaftQueryOperation> operationSupplier, QueryPolicy queryPolicy) {
            super(operationSupplier);
            this.queryPolicy = queryPolicy;
        }

        @Override
        RaftQueryOperation createOp() {
            RaftQueryOperation op = super.createOp();
            op.setQueryPolicy(queryPolicy);
            return op;
        }

        @Override
        RaftGroupId getGroupId(RaftQueryOperation op) {
            return op.getRaftGroupId();
        }
    }
}
