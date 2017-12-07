package com.hazelcast.raft.impl.service;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.ForceSetRaftNodeStatusRunnable;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftIntegration;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftNode.RaftNodeStatus;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.raft.impl.dto.InstallSnapshot;
import com.hazelcast.raft.impl.dto.PreVoteRequest;
import com.hazelcast.raft.impl.dto.PreVoteResponse;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.raft.impl.service.RaftGroupInfo.RaftGroupStatus;
import com.hazelcast.raft.impl.service.exception.CannotShutdownException;
import com.hazelcast.raft.impl.service.operation.metadata.TriggerShutdownEndpointOperation;
import com.hazelcast.raft.impl.service.proxy.DefaultRaftGroupReplicateOperation;
import com.hazelcast.raft.impl.service.proxy.RaftReplicateOperation;
import com.hazelcast.spi.ConfigurableService;
import com.hazelcast.spi.GracefulShutdownAwareService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.function.Supplier;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.impl.service.RaftCleanupHandler.CLEANUP_TASK_PERIOD;

/**
 * TODO: Javadoc Pending...
 */
public class RaftService implements ManagedService, ConfigurableService<RaftConfig>,
        SnapshotAwareService<MetadataSnapshot>, GracefulShutdownAwareService {

    public static final String SERVICE_NAME = "hz:core:raft";
    public static final RaftGroupId METADATA_GROUP_ID = RaftMetadataManager.METADATA_GROUP_ID;

    private final Map<RaftGroupId, RaftNode> nodes = new ConcurrentHashMap<RaftGroupId, RaftNode>();
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final RaftCleanupHandler cleanupHandler;

    private volatile RaftInvocationManager invocationManager;
    private volatile RaftMetadataManager metadataManager;

    private volatile RaftConfig config;

    public RaftService(NodeEngine nodeEngine) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        cleanupHandler = new RaftCleanupHandler(nodeEngine, this);
    }

    @Override
    public void configure(RaftConfig config) {
        // cloning given RaftConfig to avoid further mutations
        this.config = new RaftConfig(config);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        metadataManager = new RaftMetadataManager(nodeEngine, this, config);
        invocationManager = new RaftInvocationManager(nodeEngine, this, config);

        metadataManager.init();
        invocationManager.init();
        cleanupHandler.init();
    }

    @Override
    public void reset() {
        if (invocationManager != null) {
            invocationManager.reset();
        }
    }

    @Override
    public void shutdown(boolean terminate) {
    }

    @Override
    public MetadataSnapshot takeSnapshot(String raftName, int commitIndex) {
        return metadataManager.takeSnapshot(raftName, commitIndex);
    }

    @Override
    public void restoreSnapshot(String raftName, int commitIndex, MetadataSnapshot snapshot) {
        metadataManager.restoreSnapshot(raftName, commitIndex, snapshot);
    }

    @Override
    public boolean onShutdown(long timeout, TimeUnit unit) {
        RaftEndpoint localEndpoint = getLocalEndpoint();
        if (localEndpoint == null) {
            return true;
        }

        logger.severe("ON SHUTDOWN " + localEndpoint);

        long remainingTimeNanos = unit.toNanos(timeout);
        long start = System.nanoTime();

        ensureTriggerShutdown(localEndpoint, remainingTimeNanos);
        remainingTimeNanos -= (System.nanoTime() - start);

        // wait for us being replaced in all raft groups we are participating
        // and removed from all raft groups
        while (remainingTimeNanos > 0) {
            if (isShutdown(localEndpoint)) {
                return true;
            }
            try {
                Thread.sleep(CLEANUP_TASK_PERIOD);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            remainingTimeNanos -= CLEANUP_TASK_PERIOD;
        }
        return false;
    }

    private void ensureTriggerShutdown(RaftEndpoint localEndpoint, long remainingTimeNanos) {
        while (remainingTimeNanos > 0) {
            long start = System.nanoTime();
            try {
                // mark us as shutting-down in metadata
                Future<RaftGroupId> future = triggerShutdownEndpointAsync(localEndpoint);
                future.get(remainingTimeNanos, TimeUnit.NANOSECONDS);
                logger.severe("TRIGGERED SHUTDOWN FOR " + localEndpoint + " -> " + metadataManager.getShuttingDownEndpoint());
                return;
            } catch (CannotShutdownException e) {
                remainingTimeNanos -= (System.nanoTime() - start);
                if (remainingTimeNanos <= 0) {
                    throw e;
                }
                logger.fine(e.getMessage());
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }
    }

    public RaftMetadataManager getMetadataManager() {
        return metadataManager;
    }

    public RaftInvocationManager getInvocationManager() {
        return invocationManager;
    }

    public void handlePreVoteRequest(RaftGroupId groupId, PreVoteRequest request) {
        RaftNode node = nodes.get(groupId);
        if (node == null) {
            logger.severe("RaftNode[" + groupId.name() + "] does not exist to handle: " + request);
            return;
        }
        node.handlePreVoteRequest(request);
    }

    public void handlePreVoteResponse(RaftGroupId groupId, PreVoteResponse response) {
        RaftNode node = nodes.get(groupId);
        if (node == null) {
            logger.severe("RaftNode[" + groupId.name() + "] does not exist to handle: " + response);
            return;
        }
        node.handlePreVoteResponse(response);
    }

    public void handleVoteRequest(RaftGroupId groupId, VoteRequest request) {
        RaftNode node = nodes.get(groupId);
        if (node == null) {
            logger.severe("RaftNode[" + groupId.name() + "] does not exist to handle: " + request);
            return;
        }
        node.handleVoteRequest(request);
    }

    public void handleVoteResponse(RaftGroupId groupId, VoteResponse response) {
        RaftNode node = nodes.get(groupId);
        if (node == null) {
            logger.severe("RaftNode[" + groupId.name() + "] does not exist to handle: " + response);
            return;
        }
        node.handleVoteResponse(response);
    }

    public void handleAppendEntries(RaftGroupId groupId, AppendRequest request) {
        RaftNode node = nodes.get(groupId);
        if (node == null) {
            logger.severe("RaftNode[" + groupId.name() + "] does not exist to handle: " + request);
            return;
        }
        node.handleAppendRequest(request);
    }

    public void handleAppendResponse(RaftGroupId groupId, AppendSuccessResponse response) {
        RaftNode node = nodes.get(groupId);
        if (node == null) {
            logger.severe("RaftNode[" + groupId.name() + "] does not exist to handle: " + response);
            return;
        }
        node.handleAppendResponse(response);
    }

    public void handleAppendResponse(RaftGroupId groupId, AppendFailureResponse response) {
        RaftNode node = nodes.get(groupId);
        if (node == null) {
            logger.severe("RaftNode[" + groupId.name() + "] does not exist to handle: " + response);
            return;
        }
        node.handleAppendResponse(response);
    }

    public void handleSnapshot(RaftGroupId groupId, InstallSnapshot request) {
        RaftNode node = nodes.get(groupId);
        if (node == null) {
            logger.severe("RaftNode[" + groupId.name() + "] does not exist to handle: " + request);
            return;
        }
        node.handleInstallSnapshot(request);
    }

    public RaftNode getRaftNode(RaftGroupId groupId) {
        return nodes.get(groupId);
    }

    public RaftGroupInfo getRaftGroupInfo(RaftGroupId id) {
        return metadataManager.getRaftGroupInfo(id);
    }

    public RaftConfig getConfig() {
        return config;
    }

    public Collection<RaftEndpoint> getAllEndpoints() {
        return metadataManager.getAllEndpoints();
    }

    public RaftEndpoint getLocalEndpoint() {
        return metadataManager.getLocalEndpoint();
    }

    void createRaftNode(RaftGroupInfo groupInfo) {
        RaftGroupId groupId = groupInfo.id();
        assert nodes.get(groupId) == null : "Raft node with name " + groupId.name() + " should not exist!";

        RaftIntegration raftIntegration = new NodeEngineRaftIntegration(nodeEngine, groupId);
        RaftNode node = new RaftNode(groupInfo.serviceName(), groupId.name(), metadataManager.getLocalEndpoint(),
                groupInfo.members(), config, raftIntegration);
        nodes.put(groupId, node);
        node.start();
    }

    void destroyRaftNode(RaftGroupInfo groupInfo) {
        if (groupInfo.status() != RaftGroupStatus.DESTROYED) {
            throw new IllegalStateException(groupInfo + " must be " + RaftGroupStatus.DESTROYED);
        }
        RaftNode raftNode = nodes.remove(groupInfo.id());
        if (raftNode != null) {
            raftNode.execute(new ForceSetRaftNodeStatusRunnable(raftNode, RaftNodeStatus.TERMINATED));
            logger.info("Local raft node of " + groupInfo + " is destroyed.");
        }
    }


    private ICompletableFuture<RaftGroupId> triggerShutdownEndpointAsync(final RaftEndpoint endpoint) {
        return invocationManager.invoke(new Supplier<RaftReplicateOperation>() {
            @Override
            public RaftReplicateOperation get() {
                return new DefaultRaftGroupReplicateOperation(METADATA_GROUP_ID, new TriggerShutdownEndpointOperation(endpoint));
            }
        });
    }

    public boolean isActive(RaftEndpoint endpoint) {
        return !endpoint.equals(metadataManager.getShuttingDownEndpoint()) && !metadataManager.isShutdown(endpoint);
    }

    public boolean isShutdown(RaftEndpoint endpoint) {
        return metadataManager.isShutdown(endpoint);
    }
}
