package com.hazelcast.raft.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.RaftOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.TaskScheduler;

import java.util.concurrent.Executor;

/**
 * TODO: Javadoc Pending...
 *
 */
public final class NodeEngineRaftIntegration implements RaftIntegration {

    private final NodeEngine nodeEngine;

    public NodeEngineRaftIntegration(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public TaskScheduler getTaskScheduler() {
        return nodeEngine.getExecutionService().getGlobalTaskScheduler();
    }

    @Override
    public Executor getExecutor(String name) {
        return nodeEngine.getExecutionService().getExecutor(name);
    }

    @Override
    public ILogger getLogger(String name) {
        return nodeEngine.getLogger(name);
    }

    @Override
    public ILogger getLogger(Class clazz) {
        return nodeEngine.getLogger(clazz);
    }

    @Override
    public boolean isJoined() {
        return nodeEngine.getClusterService().isJoined();
    }

    @Override
    public boolean isReachable(RaftEndpoint endpoint) {
        return nodeEngine.getClusterService().getMember(endpoint.getAddress()) != null;
    }

    @Override
    public boolean send(Operation operation, RaftEndpoint target) {
        return nodeEngine.getOperationService().send(operation, target.getAddress());
    }

    @Override
    public Object runOperation(RaftOperation operation, int commitIndex) {
        operation.setCommitIndex(commitIndex).setNodeEngine(nodeEngine);
        // TODO do we need this ???
        OperationAccessor.setCallerAddress(operation, nodeEngine.getThisAddress());
        try {
            operation.beforeRun();
            operation.run();
            operation.afterRun();
            return operation.getResponse();
        } catch (Throwable t) {
            return t;
        }
    }
}
