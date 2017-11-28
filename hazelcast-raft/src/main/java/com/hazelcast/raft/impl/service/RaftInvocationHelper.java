package com.hazelcast.raft.impl.service;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.exception.LeaderDemotedException;
import com.hazelcast.raft.exception.NotLeaderException;
import com.hazelcast.raft.exception.RaftException;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.service.proxy.RaftReplicatingOperation;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.AbstractCompletableFuture;
import com.hazelcast.util.function.Supplier;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.impl.service.RaftService.SERVICE_NAME;

/**
 * TODO: Javadoc Pending...
 */
public final class RaftInvocationHelper {

    public static <T> ICompletableFuture<T> invokeOnLeader(NodeEngine nodeEngine,
            Supplier<RaftReplicatingOperation> operationSupplier, RaftGroupId groupId) {

        RaftService raftService = nodeEngine.getService(SERVICE_NAME);
        ILogger logger = raftService.getLogger(RaftInvocationHelper.class, groupId.name());
        Executor executor = nodeEngine.getExecutionService().getExecutor(ExecutionService.ASYNC_EXECUTOR);

        RaftInvocationFuture<T>  invocationFuture = new RaftInvocationFuture<T>(groupId, raftService, nodeEngine,
                operationSupplier, executor, logger);
        invocationFuture.invoke();
        return invocationFuture;
    }

    private static class RaftInvocationFuture<T> extends AbstractCompletableFuture<T> implements ExecutionCallback<T> {

        private final String raftName;
        private final RaftNode raftNode;
        private final RaftEndpoint[] endpoints;
        private final RaftService raftService;
        private final NodeEngine nodeEngine;
        private final Supplier<RaftReplicatingOperation> operationSupplier;
        private final ILogger logger;
        private final boolean failOnIndeterminateOperationState;
        private volatile RaftEndpoint lastInvocationEndpoint;
        private volatile int endPointIndex;

        RaftInvocationFuture(RaftGroupId groupId, RaftService raftService, NodeEngine nodeEngine,
                Supplier<RaftReplicatingOperation> operationSupplier, Executor executor, ILogger logger) {
            super(executor, logger);
            this.raftName = groupId.name();
            this.raftNode = raftService.getRaftNode(groupId);
            this.raftService = raftService;
            this.nodeEngine = nodeEngine;
            this.operationSupplier = operationSupplier;
            this.logger = logger;
            this.failOnIndeterminateOperationState = raftService.getConfig().isFailOnIndeterminateOperationState();
            this.endpoints = findEndpoints();
        }

        private RaftEndpoint[] findEndpoints() {
            if (raftNode != null) {
                return null;
            }
            RaftGroupInfo groupInfo = raftService.getRaftGroupInfo(raftName);
            if (groupInfo != null) {
                return groupInfo.membersArray();
            }
            return raftService.getAllEndpoints().toArray(new RaftEndpoint[0]);
        }

        @Override
        public void onResponse(T response) {
            if (raftNode == null) {
                raftService.setKnownLeader(raftName, lastInvocationEndpoint);
            }
            setResult(response);
        }

        @Override
        public void onFailure(Throwable cause) {
            logger.warning(cause);
            if (isRetryable(cause)) {
                if (cause instanceof RaftException) {
                    setKnownLeader((RaftException) cause);
                } else {
                    raftService.resetKnownLeader(raftName);
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

        private void setKnownLeader(RaftException cause) {
            RaftEndpoint leader = cause.getLeader();
            if (leader != null) {
                raftService.setKnownLeader(raftName, leader);
            } else {
                raftService.resetKnownLeader(raftName);
            }
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
            InternalCompletableFuture<T> future = nodeEngine.getOperationService()
                            .invokeOnTarget(SERVICE_NAME, operationSupplier.get(), leader.getAddress());
            lastInvocationEndpoint = leader;
            future.andThen(this);
        }

        private RaftEndpoint getLeader() {
            if (raftNode != null) {
                return raftNode.getLeader();
            }
            RaftEndpoint leader = raftService.getKnownLeader(raftName);
            return leader != null ? leader : endpoints[endPointIndex++ % endpoints.length];
        }
    }
}
