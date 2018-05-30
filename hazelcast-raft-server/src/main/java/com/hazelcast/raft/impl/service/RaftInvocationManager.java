package com.hazelcast.raft.impl.service;

import com.hazelcast.config.raft.RaftConfig;
import com.hazelcast.config.raft.RaftGroupConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.MembershipChangeType;
import com.hazelcast.raft.QueryPolicy;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftEndpointImpl;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.exception.CannotCreateRaftGroupException;
import com.hazelcast.raft.impl.service.operation.metadata.CreateRaftGroupOp;
import com.hazelcast.raft.impl.service.operation.metadata.GetActiveEndpointsOp;
import com.hazelcast.raft.impl.service.operation.metadata.TriggerDestroyRaftGroupOp;
import com.hazelcast.raft.impl.service.proxy.ChangeRaftGroupMembershipOp;
import com.hazelcast.raft.impl.service.proxy.DefaultRaftReplicateOp;
import com.hazelcast.raft.impl.service.proxy.RaftQueryOp;
import com.hazelcast.raft.impl.service.proxy.TerminateRaftGroupOp;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.Invocation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.impl.operationservice.impl.RaftInvocation;
import com.hazelcast.spi.impl.operationservice.impl.RaftInvocationContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executor;

import static com.hazelcast.raft.impl.service.RaftService.METADATA_GROUP_ID;
import static com.hazelcast.spi.ExecutionService.ASYNC_EXECUTOR;

@SuppressWarnings("unchecked")
public class RaftInvocationManager {

    private final NodeEngineImpl nodeEngine;
    private final OperationServiceImpl operationService;
    private final RaftService raftService;
    private final ILogger logger;
    private final RaftInvocationContext raftInvocationContext;

    RaftInvocationManager(NodeEngine nodeEngine, RaftService raftService, RaftConfig config) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.operationService = (OperationServiceImpl) nodeEngine.getOperationService();
        this.logger = nodeEngine.getLogger(getClass());
        this.raftService = raftService;
        this.raftInvocationContext = new RaftInvocationContext(logger, raftService);
    }

    void reset() {
        raftInvocationContext.reset();
    }

    public ICompletableFuture<RaftGroupId> createRaftGroup(String groupNameRef) {
        RaftGroupConfig groupConfig = raftService.getConfig().getGroupConfig(groupNameRef);
        if (groupConfig == null) {
            throw new IllegalArgumentException("No RaftGroupConfig found with name '" + groupNameRef + "'.");
        }
        return createRaftGroup(groupConfig);
    }

    public ICompletableFuture<RaftGroupId> createRaftGroup(RaftGroupConfig groupConfig) {
        return createRaftGroup(groupConfig.getName(), groupConfig.getSize());
    }

    public ICompletableFuture<RaftGroupId> createRaftGroup(String groupName, int groupSize) {
        Executor executor = nodeEngine.getExecutionService().getExecutor(ASYNC_EXECUTOR);
        ILogger logger = nodeEngine.getLogger(getClass());
        SimpleCompletableFuture<RaftGroupId> resultFuture = new SimpleCompletableFuture<RaftGroupId>(executor, logger);
        invokeGetEndpointsToCreateRaftGroup(groupName, groupSize, resultFuture);
        return resultFuture;
    }

    private void invokeGetEndpointsToCreateRaftGroup(final String groupName, final int groupSize,
                                                     final SimpleCompletableFuture<RaftGroupId> resultFuture) {
        ICompletableFuture<List<RaftEndpointImpl>> f = query(METADATA_GROUP_ID, new GetActiveEndpointsOp(), QueryPolicy.LEADER_LOCAL);

        f.andThen(new ExecutionCallback<List<RaftEndpointImpl>>() {
            @Override
            public void onResponse(List<RaftEndpointImpl> endpoints) {
                endpoints = new ArrayList<RaftEndpointImpl>(endpoints);

                if (endpoints.size() < groupSize) {
                    Exception result = new IllegalArgumentException("There are not enough active endpoints to create raft group "
                            + groupName + ". Active endpoints: " + endpoints.size() + ", Requested count: " + groupSize);
                    resultFuture.setResult(result);
                    return;
                }

                Collections.shuffle(endpoints);
                Collections.sort(endpoints, new RaftEndpointReachabilityComparator());
                endpoints = endpoints.subList(0, groupSize);
                invokeCreateRaftGroup(groupName, groupSize, endpoints, resultFuture);
            }

            @Override
            public void onFailure(Throwable t) {
                resultFuture.setResult(t);
            }
        });
    }

    private void invokeCreateRaftGroup(final String groupName, final int groupSize,
                                       final List<RaftEndpointImpl> endpoints,
                                       final SimpleCompletableFuture<RaftGroupId> resultFuture) {
        ICompletableFuture<RaftGroupId> f = invoke(METADATA_GROUP_ID, new CreateRaftGroupOp(groupName, endpoints));

        f.andThen(new ExecutionCallback<RaftGroupId>() {
            @Override
            public void onResponse(RaftGroupId groupId) {
                resultFuture.setResult(groupId);
            }

            @Override
            public void onFailure(Throwable t) {
                if (t.getCause() instanceof CannotCreateRaftGroupException) {
                    logger.fine("Could not create raft group: " + groupName + " with endpoints: " + endpoints,
                            t.getCause());
                    invokeGetEndpointsToCreateRaftGroup(groupName, groupSize, resultFuture);
                    return;
                }

                resultFuture.setResult(t);
            }
        });
    }

    public ICompletableFuture<RaftGroupId> triggerDestroyRaftGroup(final RaftGroupId groupId) {
        return invoke(METADATA_GROUP_ID, new TriggerDestroyRaftGroupOp(groupId));
    }

    <T> ICompletableFuture<T> changeRaftGroupMembership(RaftGroupId groupId, long membersCommitIndex, RaftEndpointImpl endpoint,
                                                        MembershipChangeType changeType) {
        Operation operation = new ChangeRaftGroupMembershipOp(groupId, membersCommitIndex, endpoint, changeType);
        Invocation invocation = new RaftInvocation(operationService.getInvocationContext(), raftInvocationContext,
                groupId, operation, true);
        return invocation.invoke();
    }

    public <T> ICompletableFuture<T> invoke(RaftGroupId groupId, RaftOp raftOp) {
        Operation operation = new DefaultRaftReplicateOp(groupId, raftOp);
        Invocation invocation = new RaftInvocation(operationService.getInvocationContext(), raftInvocationContext,
                groupId, operation, true);
        return invocation.invoke();
    }

    public <T> ICompletableFuture<T> query(RaftGroupId groupId, RaftOp raftOp, QueryPolicy queryPolicy) {
        RaftQueryOp operation = new RaftQueryOp(groupId, raftOp);
        operation.setQueryPolicy(queryPolicy);
        Invocation invocation = new RaftInvocation(operationService.getInvocationContext(), raftInvocationContext,
                groupId, operation, false);
        return invocation.invoke();
    }

    public <T> ICompletableFuture<T> queryOnLocal(RaftGroupId groupId, RaftOp raftOp, QueryPolicy queryPolicy) {
        RaftQueryOp queryOperation = new RaftQueryOp(groupId, raftOp);
        return nodeEngine.getOperationService().invokeOnTarget(RaftService.SERVICE_NAME,
                queryOperation.setQueryPolicy(queryPolicy), nodeEngine.getThisAddress());
    }

    public ICompletableFuture<Object> terminate(RaftGroupId groupId) {
        Operation operation = new TerminateRaftGroupOp(groupId);
        Invocation invocation = new RaftInvocation(operationService.getInvocationContext(), raftInvocationContext,
                groupId, operation, true);
        return invocation.invoke();
    }

    void setAllEndpoints(Collection<RaftEndpointImpl> endpoints) {
        raftInvocationContext.setAllEndpoints(endpoints);
    }

    private class RaftEndpointReachabilityComparator implements Comparator<RaftEndpointImpl> {
        final ClusterService clusterService = nodeEngine.getClusterService();

        @Override
        public int compare(RaftEndpointImpl o1, RaftEndpointImpl o2) {
            boolean b1 = clusterService.getMember(o1.getAddress()) != null;
            boolean b2 = clusterService.getMember(o2.getAddress()) != null;
            return b1 == b2 ? 0 : (b1 ? -1 : 1);
        }
    }
}
