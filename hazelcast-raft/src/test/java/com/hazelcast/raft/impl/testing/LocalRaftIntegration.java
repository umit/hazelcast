package com.hazelcast.raft.impl.testing;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.raft.RaftOperation;
import com.hazelcast.raft.impl.RaftDataSerializerHook;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftIntegration;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.operation.AppendFailureResponseOp;
import com.hazelcast.raft.impl.operation.AppendRequestOp;
import com.hazelcast.raft.impl.operation.AppendSuccessResponseOp;
import com.hazelcast.raft.impl.operation.AsyncRaftOp;
import com.hazelcast.raft.impl.operation.VoteRequestOp;
import com.hazelcast.raft.impl.operation.VoteResponseOp;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.TaskScheduler;
import com.hazelcast.spi.impl.executionservice.impl.DelegatingTaskScheduler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.hazelcast.test.HazelcastTestSupport.assertInstanceOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

/**
 * TODO: Javadoc Pending...
 *
 */
public class LocalRaftIntegration implements RaftIntegration {

    private final RaftEndpoint localEndpoint;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ExecutorService executorService;
    private final ConcurrentMap<RaftEndpoint, RaftNode> nodes = new ConcurrentHashMap<RaftEndpoint, RaftNode>();

    public LocalRaftIntegration(RaftEndpoint localEndpoint,
            ScheduledExecutorService scheduledExecutorService, ExecutorService executorService) {
        this.localEndpoint = localEndpoint;
        this.scheduledExecutorService = scheduledExecutorService;
        this.executorService = executorService;
    }

    public void discoverNode(RaftNode node) {
        assertNotEquals(localEndpoint, node.getLocalEndpoint());
        RaftNode old = nodes.putIfAbsent(node.getLocalEndpoint(), node);
        assertThat(old, anyOf(nullValue(), sameInstance(node)));
    }

    public boolean removeNode(RaftNode node) {
        assertNotEquals(localEndpoint, node.getLocalEndpoint());
        return nodes.remove(node.getLocalEndpoint(), node);
    }

    public RaftEndpoint getLocalEndpoint() {
        return localEndpoint;
    }

    @Override
    public TaskScheduler getTaskScheduler() {
        return new DelegatingTaskScheduler(scheduledExecutorService, executorService);
    }

    @Override
    public Executor getExecutor(String name) {
        return executorService;
    }

    @Override
    public ILogger getLogger(String name) {
        return Logger.getLogger(name);
    }

    @Override
    public ILogger getLogger(Class clazz) {
        return Logger.getLogger(clazz);
    }

    @Override
    public boolean isJoined() {
        return true;
    }

    @Override
    public boolean isReachable(RaftEndpoint endpoint) {
        if (localEndpoint.equals(endpoint)) {
            return true;
        }
        return nodes.containsKey(endpoint);
    }

    @Override
    public boolean send(Operation operation, RaftEndpoint target) {
        assertNotEquals(localEndpoint, target);
        RaftNode node = nodes.get(target);
        if (node == null) {
            return false;
        }

        assertInstanceOf(AsyncRaftOp.class, operation);

        switch (((AsyncRaftOp) operation).getId()) {
            case RaftDataSerializerHook.VOTE_REQUEST_OP:
                node.handleVoteRequest(((VoteRequestOp) operation).getVoteRequest());
                break;
            case RaftDataSerializerHook.VOTE_RESPONSE_OP:
                node.handleVoteResponse(((VoteResponseOp) operation).getVoteResponse());
                break;
            case RaftDataSerializerHook.APPEND_REQUEST_OP:
                node.handleAppendRequest(((AppendRequestOp) operation).getAppendRequest());
                break;
            case RaftDataSerializerHook.APPEND_SUCCESS_RESPONSE_OP:
                node.handleAppendResponse(((AppendSuccessResponseOp) operation).getAppendResponse());
                break;
            case RaftDataSerializerHook.APPEND_FAILURE_RESPONSE_OP:
                node.handleAppendResponse(((AppendFailureResponseOp) operation).getAppendResponse());
                break;
            default:
                throw new IllegalArgumentException("Unknown operation: " + operation);
        }
        return true;
    }

    @Override
    public Object runOperation(RaftOperation operation, int commitIndex) {
        if (operation == null) {
            return null;
        }
        operation.setCommitIndex(commitIndex);
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
