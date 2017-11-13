package com.hazelcast.raft.impl.service;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.LeaderDemotedException;
import com.hazelcast.raft.NotLeaderException;
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
            Supplier<RaftReplicatingOperation> operationSupplier, String raftName) {

        RaftService raftService = nodeEngine.getService(SERVICE_NAME);
        RaftNode raftNode = raftService.getRaftNode(raftName);
        RaftGroupInfo groupInfo = raftService.getRaftGroupInfo(raftName);
        if (raftNode == null && groupInfo == null) {
            throw new IllegalArgumentException(raftName + " raft group does not exist!");
        }

        ILogger logger = raftService.getLogger(RaftInvocationHelper.class, raftName);
        Executor executor = nodeEngine.getExecutionService().getExecutor(ExecutionService.ASYNC_EXECUTOR);

        RaftInvocationFuture<T>  invocationFuture = new RaftInvocationFuture<T>(raftNode, groupInfo, raftService,
                nodeEngine, operationSupplier, executor, logger);
        invocationFuture.invoke();
        return invocationFuture;
    }

    private static class RaftInvocationFuture<T> extends AbstractCompletableFuture<T> implements ExecutionCallback<T> {

        private final RaftNode raftNode;
        private final RaftGroupInfo groupInfo;
        private final RaftService raftService;
        private final NodeEngine nodeEngine;
        private final Supplier<RaftReplicatingOperation> operationSupplier;
        private final ILogger logger;
        private final ContinuousEndpointIterator endpointIterator;
        private volatile RaftEndpoint leader;

        RaftInvocationFuture(RaftNode raftNode, RaftGroupInfo groupInfo, RaftService raftService, NodeEngine nodeEngine,
                Supplier<RaftReplicatingOperation> operationSupplier, Executor executor, ILogger logger) {
            super(executor, logger);
            this.raftNode = raftNode;
            this.groupInfo = groupInfo;
            this.raftService = raftService;
            this.nodeEngine = nodeEngine;
            this.operationSupplier = operationSupplier;
            this.logger = logger;

            if (raftNode == null) {
                endpointIterator = new ContinuousEndpointIterator(groupInfo.getMembers().toArray(new RaftEndpoint[0]));
            } else {
                endpointIterator = null;
            }
        }

        @Override
        public void onResponse(T response) {
            if (raftNode == null) {
                raftService.setKnownLeader(groupInfo.getName(), leader);
            }
            setResult(response);
        }

        @Override
        public void onFailure(Throwable cause) {
            logger.warning(cause);
            if (cause instanceof NotLeaderException
                    || cause instanceof LeaderDemotedException
                    || cause instanceof MemberLeftException
                    || cause instanceof CallerNotMemberException
                    || cause instanceof TargetNotMemberException) {

                try {
                    raftService.resetKnownLeader(groupInfo.getName());
                    // TODO: needs a back-off strategy
                    nodeEngine.getExecutionService().schedule(new Runnable() {
                        @Override
                        public void run() {
                            invoke();
                        }
                    }, 250, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    logger.warning(e);
                    setResult(e);
                }
            } else {
                setResult(cause);
            }
        }

        void invoke() {
            leader = getLeader();
            Address target = leader != null ? leader.getAddress() : null;
            InternalCompletableFuture<T>  future = nodeEngine.getOperationService()
                            .invokeOnTarget(SERVICE_NAME, operationSupplier.get(), target);
            future.andThen(this);
        }

        private RaftEndpoint getLeader() {
            RaftEndpoint leader;
            if (raftNode != null) {
                return raftNode.getLeader();
            }
            leader = raftService.getKnownLeader(groupInfo.getName());
            return leader != null ? leader : endpointIterator.next();
        }
    }

    private static class ContinuousEndpointIterator {
        private final RaftEndpoint[] endpoints;
        private int index;

        private ContinuousEndpointIterator(RaftEndpoint[] endpoints) {
            this.endpoints = endpoints;
        }

        RaftEndpoint next() {
            return endpoints[index++ % endpoints.length];
        }
    }
}
