package com.hazelcast.raft.impl.service;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.exception.LeaderDemotedException;
import com.hazelcast.raft.exception.NotLeaderException;
import com.hazelcast.raft.exception.RaftException;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.service.operation.metadata.TriggerDestroyRaftGroupOperation;
import com.hazelcast.raft.impl.service.proxy.CreateRaftGroupReplicateOperation;
import com.hazelcast.raft.impl.service.proxy.DefaultRaftGroupReplicateOperation;
import com.hazelcast.raft.impl.service.proxy.RaftReplicateOperation;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.AbstractCompletableFuture;
import com.hazelcast.util.function.Supplier;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.impl.service.RaftService.METADATA_GROUP_ID;

public class RaftInvocationManager {

    private final NodeEngine nodeEngine;
    private final RaftService raftService;
    private final ILogger logger;
    private final ConcurrentMap<RaftGroupId, RaftEndpoint> knownLeaders = new ConcurrentHashMap<RaftGroupId, RaftEndpoint>();
    private final RaftEndpoint[] allEndpoints;
    private final boolean failOnIndeterminateOperationState;

    public RaftInvocationManager(NodeEngine nodeEngine, RaftService raftService, RaftConfig config) {
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

    public ICompletableFuture<RaftGroupId> createRaftGroupAsync(final String serviceName, final String raftName,
                                                                final int nodeCount) {
        return invoke(new Supplier<RaftReplicateOperation>() {
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

    public ICompletableFuture<RaftGroupId> triggerDestroyRaftGroupAsync(final RaftGroupId groupId) {
        return invoke(new Supplier<RaftReplicateOperation>() {
            @Override
            public RaftReplicateOperation get() {
                return new DefaultRaftGroupReplicateOperation(METADATA_GROUP_ID, new TriggerDestroyRaftGroupOperation(groupId));
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

    private class RaftInvocationFuture<T> extends AbstractCompletableFuture<T>
            implements ExecutionCallback<T> {

        private final Supplier<RaftReplicateOperation> operationSupplier;
        private volatile RaftGroupId groupId;
        private volatile RaftEndpoint lastInvocationEndpoint;
        private volatile RaftEndpoint[] endpoints;
        private volatile int endPointIndex;

        RaftInvocationFuture(Supplier<RaftReplicateOperation> operationSupplier) {
            super(nodeEngine, RaftInvocationManager.this.logger);
            this.operationSupplier = operationSupplier;
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
            RaftReplicateOperation op = operationSupplier.get();
            groupId = op.getRaftGroupId();
            RaftEndpoint leader = getLeader();
            if (leader == null) {
                scheduleRetry();
                return;
            }


            InternalCompletableFuture<T> future = nodeEngine.getOperationService()
                                                            .invokeOnTarget(RaftService.SERVICE_NAME, op, leader.getAddress());
            lastInvocationEndpoint = leader;
            future.andThen(this);
        }

        private RaftEndpoint getLeader() {
            RaftEndpoint leader = getKnownLeader(groupId);
            if (leader != null) {
                return leader;
            }

            if (endpoints == null || endPointIndex == endpoints.length) {
                RaftGroupInfo raftGroupInfo = raftService.getRaftGroupInfo(groupId);
                endpoints = raftGroupInfo != null ? raftGroupInfo.membersArray() : allEndpoints;
                endPointIndex = 0;
            }

            return endpoints != null ? endpoints[endPointIndex++] : null;
        }
    }
}
