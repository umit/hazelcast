package com.hazelcast.raft.impl.service;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.RaftMember;
import com.hazelcast.raft.RaftOperation;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.exception.RaftGroupTerminatedException;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftIntegration;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.raft.impl.dto.InstallSnapshot;
import com.hazelcast.raft.impl.dto.PreVoteRequest;
import com.hazelcast.raft.impl.dto.PreVoteResponse;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.raft.impl.operation.TerminateRaftGroupOp;
import com.hazelcast.raft.impl.service.RaftGroupInfo.RaftGroupStatus;
import com.hazelcast.raft.impl.service.operation.metadata.CompleteDestroyRaftGroupsOperation;
import com.hazelcast.raft.impl.service.operation.metadata.GetDestroyingRaftGroupsOperation;
import com.hazelcast.raft.impl.service.proxy.DefaultRaftGroupReplicatingOperation;
import com.hazelcast.raft.impl.service.proxy.RaftReplicatingOperation;
import com.hazelcast.spi.ConfigurableService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.AddressUtil;
import com.hazelcast.util.function.Supplier;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.impl.service.util.RaftInvocationHelper.invokeOnLeader;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.Collections.emptyList;

/**
 * TODO: Javadoc Pending...
 */
public class RaftService implements ManagedService, ConfigurableService<RaftConfig>,
        SnapshotAwareService<ArrayList<RaftGroupInfo>> {

    public static final String SERVICE_NAME = "hz:core:raft";
    private static final String METADATA_RAFT = "METADATA";
    public static final RaftGroupId METADATA_GROUP_ID = new RaftGroupId(METADATA_RAFT, 0);

    private final Map<RaftGroupId, RaftGroupInfo> raftGroups = new ConcurrentHashMap<RaftGroupId, RaftGroupInfo>();
    private final Map<RaftGroupId, RaftNode> nodes = new ConcurrentHashMap<RaftGroupId, RaftNode>();
    private final ConcurrentMap<RaftGroupId, RaftEndpoint> knownLeaders = new ConcurrentHashMap<RaftGroupId, RaftEndpoint>();
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;

    private volatile RaftConfig config;
    private volatile Collection<RaftEndpoint> endpoints;
    private volatile RaftEndpoint localEndpoint;

    public RaftService(NodeEngine nodeEngine) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        try {
            endpoints = Collections.unmodifiableCollection(parseEndpoints());
        } catch (UnknownHostException e) {
            throw new HazelcastException(e);
        }
        logger.info("CP nodes: " + endpoints);
        raftGroups.put(METADATA_GROUP_ID, new RaftGroupInfo(METADATA_GROUP_ID, endpoints, SERVICE_NAME));

        localEndpoint = findLocalEndpoint(endpoints);
        if (localEndpoint == null) {
            logger.warning("We are not in CP nodes group :(");
            return;
        }

        RaftIntegration raftIntegration = new NodeEngineRaftIntegration(this.nodeEngine, METADATA_GROUP_ID);
        RaftNode node = new RaftNode(SERVICE_NAME, METADATA_RAFT, localEndpoint, endpoints, config, raftIntegration);
        nodes.put(METADATA_GROUP_ID, node);
        node.start();

        ExecutionService executionService = nodeEngine.getExecutionService();

        executionService.scheduleWithRepetition(new CleanupTask(), 1000,1000, TimeUnit.MILLISECONDS);
    }

    private RaftEndpoint findLocalEndpoint(Collection<RaftEndpoint> endpoints) {
        for (RaftEndpoint endpoint : endpoints) {
            if (nodeEngine.getThisAddress().equals(endpoint.getAddress())) {
                return endpoint;
            }
        }
        return null;
    }

    private Collection<RaftEndpoint> parseEndpoints() throws UnknownHostException {
        Collection<RaftMember> members = config.getMembers();
        Set<RaftEndpoint> endpoints = new HashSet<RaftEndpoint>(members.size());
        for (RaftMember member : members) {
            AddressUtil.AddressHolder addressHolder = AddressUtil.getAddressHolder(member.getAddress());
            Address address = new Address(addressHolder.getAddress(), addressHolder.getPort());
            address.setScopeId(addressHolder.getScopeId());
            endpoints.add(new RaftEndpoint(member.getId(), address));
        }
        return endpoints;
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
    }

    @Override
    public void configure(RaftConfig config) {
        // cloning given RaftConfig to avoid further mutations
        this.config = new RaftConfig(config);
    }

    @Override
    public ArrayList<RaftGroupInfo> takeSnapshot(String raftName, int commitIndex) {
        assert METADATA_RAFT.equals(raftName);

        ArrayList<RaftGroupInfo> groupInfos = new ArrayList<RaftGroupInfo>();
        for (RaftGroupInfo groupInfo : raftGroups.values()) {
            assert groupInfo.commitIndex() <= commitIndex
                    : "Group commit index: " + groupInfo.commitIndex() + ", snapshot commit index: " + commitIndex;
            if (!METADATA_RAFT.equals(groupInfo.name())) {
                groupInfos.add(groupInfo);
            }
        }
        return groupInfos;
    }

    @Override
    public void restoreSnapshot(String raftName, int commitIndex, ArrayList<RaftGroupInfo> snapshot) {
        assert METADATA_RAFT.equals(raftName);

        for (RaftGroupInfo groupInfo : snapshot) {
            if (!raftGroups.containsKey(groupInfo.id())) {
                createRaftGroup(groupInfo.serviceName(), groupInfo.name(), groupInfo.members(), groupInfo.commitIndex());
            }
        }
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


    public ILogger getLogger(Class clazz, String raftName) {
        return nodeEngine.getLogger(clazz.getName() + "(" + raftName + ")");
    }

    public RaftNode getRaftNode(RaftGroupId groupId) {
        return nodes.get(groupId);
    }

    public RaftGroupInfo getRaftGroupInfo(RaftGroupId id) {
        return raftGroups.get(id);
    }

    public RaftConfig getConfig() {
        return config;
    }

    public Collection<RaftEndpoint> getAllEndpoints() {
        return endpoints;
    }

    public RaftEndpoint getLocalEndpoint() {
        return localEndpoint;
    }

    public RaftGroupId createRaftGroup(String serviceName, String name, Collection<RaftEndpoint> endpoints, int commitIndex) {
        // keep configuration on every metadata node
        RaftGroupInfo groupInfo = getRaftGroupInfoByName(name);
        if (groupInfo != null) {
            if (groupInfo.members().size() == endpoints.size()) {
                logger.warning("Raft group " + name + " already exists. Ignoring add raft node request.");
                return groupInfo.id();
            }

            throw new IllegalStateException("Raft group " + name
                    + " already exists with different group size. Ignoring add raft node request.");
        }

        RaftGroupId groupId = new RaftGroupId(name, commitIndex);
        raftGroups.put(groupId, new RaftGroupInfo(groupId, endpoints, serviceName));

        if (!endpoints.contains(localEndpoint)) {
            return groupId;
        }

        assert nodes.get(groupId) == null : "Raft node with name " + name + " should not exist!";

        RaftIntegration raftIntegration = new NodeEngineRaftIntegration(nodeEngine, groupId);
        RaftNode node = new RaftNode(serviceName, name, localEndpoint, endpoints, config, raftIntegration);
        nodes.put(groupId, node);
        node.start();
        return groupId;
    }

    public void triggerDestroy(RaftGroupId groupId) {
        RaftGroupInfo groupInfo = raftGroups.get(groupId);
        checkNotNull(groupInfo, "No raft group exists for " + groupId + " to trigger destroy");

        if (groupInfo.setDestroying()) {
            logger.info("Destroying " + groupId);
        } else {
            logger.info(groupId + " is already " + groupInfo.status());
        }
    }

    public Collection<RaftGroupId> getDestroyingRaftGroupIds() {
        Collection<RaftGroupId> groupIds = new ArrayList<RaftGroupId>();
        for (RaftGroupInfo groupInfo : raftGroups.values()) {
            if (groupInfo.status() == RaftGroupStatus.DESTROYING) {
                groupIds.add(groupInfo.id());
            }
        }

        return groupIds;
    }

    public void completeDestroy(Set<RaftGroupId> groupIds) {
        for (RaftGroupId groupId : groupIds) {
            completeDestroy(groupId);
        }
    }

    private void completeDestroy(RaftGroupId groupId) {
        RaftGroupInfo groupInfo = raftGroups.get(groupId);
        checkNotNull(groupInfo, "No raft group exists for " + groupId + " to commit destroy");

        if (groupInfo.setDestroyed()) {
            logger.info(groupId + " is destroyed.");
            RaftNode raftNode = nodes.remove(groupId);
            if (raftNode != null) {
                logger.info("Local raft node of " + groupId + " is destroyed.");
            }
        }
    }

    private RaftGroupInfo getRaftGroupInfoByName(String name) {
        for (RaftGroupInfo groupInfo : raftGroups.values()) {
            if (groupInfo.status() != RaftGroupStatus.DESTROYED && groupInfo.name().equals(name)) {
                return groupInfo;
            }
        }
        return null;
    }

    public void resetKnownLeader(RaftGroupId groupId) {
        logger.fine("Resetting known leader for raft: " + groupId);
        knownLeaders.remove(groupId);
    }

    public void setKnownLeader(RaftGroupId groupId, RaftEndpoint leader) {
        logger.fine("Setting known leader for raft: " + groupId + " to " + leader);
        knownLeaders.put(groupId, leader);
    }

    public RaftEndpoint getKnownLeader(RaftGroupId groupId) {
        return knownLeaders.get(groupId);
    }

    private class CleanupTask implements Runnable {

        @Override
        public void run() {
            if (!shouldRun()) {
                return;
            }

            Map<RaftGroupId, Future<Object>> futures = new HashMap<RaftGroupId, Future<Object>>();

            for (final RaftGroupId groupId : getDestroyingRaftGroupIds()) {
                Future<Object> future = invokeOnLeader(nodeEngine, new Supplier<RaftReplicatingOperation>() {
                            @Override
                            public RaftReplicatingOperation get() {
                                return new DefaultRaftGroupReplicatingOperation(groupId, new TerminateRaftGroupOp());
                            }
                        }, groupId);
                futures.put(groupId, future);
            }

            final Set<RaftGroupId> terminatedGroupIds = new HashSet<RaftGroupId>();
            for (Entry<RaftGroupId, Future<Object>> e : futures.entrySet()) {
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
            Future<Collection<RaftGroupId>> f = invokeOnLeader(nodeEngine, new Supplier<RaftReplicatingOperation>() {
                @Override
                public RaftReplicatingOperation get() {
                    return new DefaultRaftGroupReplicatingOperation(METADATA_GROUP_ID, new GetDestroyingRaftGroupsOperation());
                }
            }, METADATA_GROUP_ID);
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
            Future<Collection<RaftGroupId>> f = invokeOnLeader(nodeEngine, new Supplier<RaftReplicatingOperation>() {
                @Override
                public RaftReplicatingOperation get() {
                    RaftOperation raftOperation = new CompleteDestroyRaftGroupsOperation(destroyedGroupIds);
                    return new DefaultRaftGroupReplicatingOperation(METADATA_GROUP_ID, raftOperation);
                }
            }, METADATA_GROUP_ID);

            try {
                f.get();
                logger.info("Terminated raft groups: " + destroyedGroupIds + " are committed.");
            } catch (Exception e) {
                logger.severe("Cannot commit terminated raft groups: " + destroyedGroupIds, e);
            }
        }
    }
}
