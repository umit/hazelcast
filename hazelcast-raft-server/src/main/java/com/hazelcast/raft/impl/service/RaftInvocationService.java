package com.hazelcast.raft.impl.service;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.RaftOperation;
import com.hazelcast.raft.exception.LeaderDemotedException;
import com.hazelcast.raft.exception.NotLeaderException;
import com.hazelcast.raft.exception.RaftException;
import com.hazelcast.raft.exception.RaftGroupTerminatedException;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.operation.TerminateRaftGroupOp;
import com.hazelcast.raft.impl.service.operation.metadata.CompleteDestroyRaftGroupsOperation;
import com.hazelcast.raft.impl.service.operation.metadata.CreateRaftGroupReplicateOperation;
import com.hazelcast.raft.impl.service.operation.metadata.GetDestroyingRaftGroupsOperation;
import com.hazelcast.raft.impl.service.operation.metadata.TriggerDestroyRaftGroupOperation;
import com.hazelcast.raft.impl.service.proxy.DefaultRaftGroupReplicateOperation;
import com.hazelcast.raft.impl.service.proxy.RaftReplicateOperation;
import com.hazelcast.spi.ConfigurableService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.AbstractCompletableFuture;
import com.hazelcast.util.function.Supplier;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.impl.service.RaftService.METADATA_GROUP_ID;
import static java.util.Collections.emptyList;

public class RaftInvocationService implements ManagedService, ConfigurableService<RaftConfig>  {

    public static final String SERVICE_NAME = "hz:core:raftInvocation";

    private final NodeEngine nodeEngine;
    private final ILogger logger;
    private final ConcurrentMap<RaftGroupId, RaftEndpoint> knownLeaders = new ConcurrentHashMap<RaftGroupId, RaftEndpoint>();
    private volatile RaftConfig raftConfig;
    private volatile RaftEndpoint[] endpoints;

    public RaftInvocationService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
    }

    @Override
    public void configure(RaftConfig raftConfig) {
        try {
            this.raftConfig = raftConfig;
            this.endpoints = RaftEndpoint.parseEndpoints(raftConfig.getMembers()).toArray(new RaftEndpoint[0]);
        } catch (UnknownHostException e) {
            throw new HazelcastException(e);
        }
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.scheduleWithRepetition(new CleanupTask(), 1000,1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void reset() {
        knownLeaders.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    public ICompletableFuture<RaftGroupId> createRaftGroupAsync(final String serviceName, final String raftName,
                                                                final int nodeCount) {
        return invoke(METADATA_GROUP_ID, new Supplier<RaftReplicateOperation>() {
            @Override
            public RaftReplicateOperation get() {
                return new CreateRaftGroupReplicateOperation(serviceName, raftName, nodeCount);
            }
        });
    }

    public RaftGroupId createRaftGroup(String serviceName, String raftName, int nodeCount)
            throws ExecutionException, InterruptedException {
        return createRaftGroupAsync(serviceName, raftName, nodeCount).get();
    }

    public ICompletableFuture<RaftGroupId> destroyRaftGroupAsync(final RaftGroupId groupId) {
        return invoke(METADATA_GROUP_ID, new Supplier<RaftReplicateOperation>() {
            @Override
            public RaftReplicateOperation get() {
                return new DefaultRaftGroupReplicateOperation(METADATA_GROUP_ID, new TriggerDestroyRaftGroupOperation(groupId));
            }
        });
    }

    public void destroyRaftGroup(final RaftGroupId groupId) throws ExecutionException, InterruptedException {
        destroyRaftGroupAsync(groupId).get();
    }

    public <T> ICompletableFuture<T> invoke(RaftGroupId groupId, Supplier<RaftReplicateOperation> operationSupplier) {
        Executor executor = nodeEngine.getExecutionService().getExecutor(ExecutionService.ASYNC_EXECUTOR);

        RaftInvocationFuture<T> invocationFuture = new RaftInvocationFuture<T>(groupId, nodeEngine,
                operationSupplier, executor, logger);
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

    private class RaftInvocationFuture<T> extends AbstractCompletableFuture<T>
            implements ExecutionCallback<T> {

        private final RaftGroupId groupId;
        private final NodeEngine nodeEngine;
        private final Supplier<RaftReplicateOperation> operationSupplier;
        private final ILogger logger;
        private final boolean failOnIndeterminateOperationState;
        private volatile RaftEndpoint lastInvocationEndpoint;
        private volatile int endPointIndex;

        RaftInvocationFuture(RaftGroupId groupId, NodeEngine nodeEngine,
                             Supplier<RaftReplicateOperation> operationSupplier, Executor executor, ILogger logger) {
            super(executor, logger);
            this.groupId = groupId;
            this.nodeEngine = nodeEngine;
            this.operationSupplier = operationSupplier;
            this.logger = logger;
            this.failOnIndeterminateOperationState = raftConfig.isFailOnIndeterminateOperationState();
        }

        @Override
        public void onResponse(T response) {
            setKnownLeader(groupId, lastInvocationEndpoint);
            setResult(response);
        }

        @Override
        public void onFailure(Throwable cause) {
            logger.warning(cause);
            if (isRetryable(cause)) {
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

        private boolean isRetryable(Throwable cause) {
            if (failOnIndeterminateOperationState && cause instanceof MemberLeftException) {
                return false;
            }
            return cause instanceof NotLeaderException
                    || cause instanceof LeaderDemotedException
                    || cause instanceof MemberLeftException
                    || cause instanceof CallerNotMemberException
                    || cause instanceof TargetNotMemberException;
        }

        private void scheduleRetry() {
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

        void invoke() {
            RaftEndpoint leader = getLeader();
            if (leader == null) {
                scheduleRetry();
                return;
            }
            RaftReplicateOperation op = operationSupplier.get();
            InternalCompletableFuture<T> future = nodeEngine.getOperationService()
                                                            .invokeOnTarget(RaftService.SERVICE_NAME, op, leader.getAddress());
            lastInvocationEndpoint = leader;
            future.andThen(this);
        }

        private RaftEndpoint getLeader() {
            RaftEndpoint leader = getKnownLeader(groupId);
            return leader != null ? leader : endpoints != null ? endpoints[endPointIndex++ % endpoints.length] : null;
        }
    }

    private class CleanupTask implements Runnable {

        @Override
        public void run() {
            if (!shouldRun()) {
                return;
            }

            Map<RaftGroupId, Future<Object>> futures = new HashMap<RaftGroupId, Future<Object>>();

            for (final RaftGroupId groupId : getDestroyingRaftGroupIds()) {
                Future<Object> future = invoke(groupId, new Supplier<RaftReplicateOperation>() {
                    @Override
                    public RaftReplicateOperation get() {
                        return new DefaultRaftGroupReplicateOperation(groupId, new TerminateRaftGroupOp());
                    }
                });
                futures.put(groupId, future);
            }

            final Set<RaftGroupId> terminatedGroupIds = new HashSet<RaftGroupId>();
            for (Map.Entry<RaftGroupId, Future<Object>> e : futures.entrySet()) {
                if (isTerminated(e.getKey(), e.getValue())) {
                    terminatedGroupIds.add(e.getKey());
                }
            }

            if (terminatedGroupIds.isEmpty()) {
                return;
            }

            commitDestroyedRaftGroups(terminatedGroupIds);
        }

        private boolean shouldRun() {
            ClusterService clusterService = nodeEngine.getClusterService();
            return nodeEngine.isRunning() && clusterService.isJoined() && clusterService.isMaster();
        }

        private Collection<RaftGroupId> getDestroyingRaftGroupIds() {
            Future<Collection<RaftGroupId>> f = invoke(METADATA_GROUP_ID, new Supplier<RaftReplicateOperation>() {
                @Override
                public RaftReplicateOperation get() {
                    return new DefaultRaftGroupReplicateOperation(METADATA_GROUP_ID, new GetDestroyingRaftGroupsOperation());
                }
            });
            try {
                return f.get();
            } catch (Exception e) {
                logger.severe("Cannot fetch destroying raft group ids", e);
                return emptyList();
            }
        }

        private boolean isTerminated(RaftGroupId groupId, Future<Object> future) {
            try {
                future.get();
                return true;
            }  catch (InterruptedException e) {
                logger.severe("Cannot get result of DESTROY commit to " + groupId, e);
                return false;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof RaftGroupTerminatedException) {
                    return true;
                }

                logger.severe("Cannot get result of DESTROY commit to " + groupId, e);

                return false;
            }
        }

        private void commitDestroyedRaftGroups(final Set<RaftGroupId> destroyedGroupIds) {
            Future<Collection<RaftGroupId>> f = invoke(METADATA_GROUP_ID, new Supplier<RaftReplicateOperation>() {
                @Override
                public RaftReplicateOperation get() {
                    RaftOperation raftOperation = new CompleteDestroyRaftGroupsOperation(destroyedGroupIds);
                    return new DefaultRaftGroupReplicateOperation(METADATA_GROUP_ID, raftOperation);
                }
            });

            try {
                f.get();
                logger.info("Terminated raft groups: " + destroyedGroupIds + " are committed.");
            } catch (Exception e) {
                logger.severe("Cannot commit terminated raft groups: " + destroyedGroupIds, e);
            }
        }
    }

}