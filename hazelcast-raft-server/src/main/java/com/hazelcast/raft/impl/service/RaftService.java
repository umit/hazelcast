package com.hazelcast.raft.impl.service;

import com.hazelcast.core.HazelcastException;
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
import com.hazelcast.spi.ConfigurableService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.util.Preconditions.checkNotNull;

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
            endpoints = Collections.unmodifiableCollection(RaftEndpoint.parseEndpoints(config.getMembers()));
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
    }

    private RaftEndpoint findLocalEndpoint(Collection<RaftEndpoint> endpoints) {
        for (RaftEndpoint endpoint : endpoints) {
            if (nodeEngine.getThisAddress().equals(endpoint.getAddress())) {
                return endpoint;
            }
        }
        return null;
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
        // TODO [basri] this is broken. what about destroyed ones ???
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
                raftNode.execute(new ForceSetRaftNodeStatusRunnable(raftNode, RaftNodeStatus.TERMINATED));
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
}
