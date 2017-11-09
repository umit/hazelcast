package com.hazelcast.raft.impl.testing;

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftOperation;
import com.hazelcast.raft.impl.RaftDataSerializerHook;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftIntegration;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.raft.impl.operation.AsyncRaftOp;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.TaskScheduler;
import com.hazelcast.spi.impl.executionservice.impl.DelegatingTaskScheduler;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Map;
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 * TODO: Javadoc Pending...
 *
 */
public class LocalRaftIntegration implements RaftIntegration {

    private final RaftEndpoint localEndpoint;
    private final Map<String, Object> services;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ExecutorService executorService;
    private final ConcurrentMap<RaftEndpoint, RaftNode> nodes = new ConcurrentHashMap<RaftEndpoint, RaftNode>();
    private final SerializationService serializationService = new DefaultSerializationServiceBuilder().build();

    public LocalRaftIntegration(RaftEndpoint localEndpoint, Map<String, Object> services,
            ScheduledExecutorService scheduledExecutorService, ExecutorService executorService) {
        this.localEndpoint = localEndpoint;
        this.services = services;
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

        IdentifiedDataSerializable payload = ((AsyncRaftOp) operation).getPayload();
        assertNotNull(payload);
        payload = serializationService.toObject(serializationService.toData(payload));

        switch (payload.getId()) {
            case RaftDataSerializerHook.VOTE_REQUEST:
                node.handleVoteRequest((VoteRequest) payload);
                break;
            case RaftDataSerializerHook.VOTE_RESPONSE:
                node.handleVoteResponse((VoteResponse) payload);
                break;
            case RaftDataSerializerHook.APPEND_REQUEST:
                node.handleAppendRequest((AppendRequest) payload);
                break;
            case RaftDataSerializerHook.APPEND_SUCCESS_RESPONSE:
                node.handleAppendResponse((AppendSuccessResponse) payload);
                break;
            case RaftDataSerializerHook.APPEND_FAILURE_RESPONSE:
                node.handleAppendResponse((AppendFailureResponse) payload);
                break;
            default:
                throw new IllegalArgumentException("Unknown payload: " + payload);
        }
        return true;
    }

    @Override
    public Object runOperation(RaftOperation operation, int commitIndex) {
        if (operation == null) {
            return null;
        }
        if (operation.getServiceName() != null) {
            operation.setService(services.get(operation.getServiceName()));
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
