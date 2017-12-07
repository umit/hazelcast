package com.hazelcast.raft.impl.service;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.service.RaftGroupInfo.RaftGroupStatus;
import com.hazelcast.raft.impl.service.exception.CannotShutdownException;
import com.hazelcast.spi.NodeEngine;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.raft.impl.RaftEndpoint.parseEndpoints;
import static com.hazelcast.raft.impl.service.RaftService.SERVICE_NAME;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.Collections.newSetFromMap;
import static java.util.Collections.unmodifiableCollection;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftMetadataManager implements SnapshotAwareService<MetadataSnapshot>  {

    private static final String METADATA_RAFT = "METADATA";
    public static final RaftGroupId METADATA_GROUP_ID = new RaftGroupId(METADATA_RAFT, 0);

    private final NodeEngine nodeEngine;
    private final RaftService raftService;
    private final ILogger logger;

    // raftGroups are read outside of Raft
    private final Map<RaftGroupId, RaftGroupInfo> raftGroups = new ConcurrentHashMap<RaftGroupId, RaftGroupInfo>();
    private final Collection<RaftEndpoint> allEndpoints;
    private final RaftEndpoint localEndpoint;

    // read outside of Raft
    private final Collection<RaftEndpoint> shutdownEndpoints = newSetFromMap(new ConcurrentHashMap<RaftEndpoint, Boolean>());
    // read outside of Raft
    private volatile RaftEndpoint shuttingDownEndpoint;

    public RaftMetadataManager(NodeEngine nodeEngine, RaftService raftService, RaftConfig config) {
        this.nodeEngine = nodeEngine;
        this.raftService = raftService;
        this.logger = nodeEngine.getLogger(getClass());

        try {
            this.allEndpoints = unmodifiableCollection(parseEndpoints(config.getMembers()));
        } catch (UnknownHostException e) {
            throw new HazelcastException(e);
        }
        this.localEndpoint = findLocalEndpoint(allEndpoints);
    }

    public void init() {
        logger.info("CP nodes: " + allEndpoints + ", local: " + localEndpoint);
        if (localEndpoint == null) {
            logger.warning("We are not in CP nodes group :(");
            return;
        }
        createRaftGroup(new RaftGroupInfo(METADATA_GROUP_ID, allEndpoints, SERVICE_NAME));
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
    public MetadataSnapshot takeSnapshot(String raftName, int commitIndex) {
        assert METADATA_RAFT.equals(raftName);

        MetadataSnapshot snapshot = new MetadataSnapshot();
        for (RaftGroupInfo groupInfo : raftGroups.values()) {
            assert groupInfo.commitIndex() <= commitIndex
                    : "Group commit index: " + groupInfo.commitIndex() + ", snapshot commit index: " + commitIndex;
            snapshot.addRaftGroup(groupInfo);
        }
        for (RaftEndpoint endpoint : allEndpoints) {
            snapshot.addEndpoint(endpoint);
        }
        for (RaftEndpoint endpoint : shutdownEndpoints) {
            snapshot.addShutdownEndpoint(endpoint);
        }
        snapshot.setShuttingDownEndpoint(shuttingDownEndpoint);
        return snapshot;
    }

    @Override
    public void restoreSnapshot(String raftName, int commitIndex, MetadataSnapshot snapshot) {
        assert METADATA_RAFT.equals(raftName);
        for (RaftGroupInfo groupInfo : snapshot.getRaftGroups()) {
            RaftGroupInfo existingGroupInfo = raftGroups.get(groupInfo.id());

            if (groupInfo.status() == RaftGroupStatus.ACTIVE && existingGroupInfo == null) {
                createRaftGroup(groupInfo);
                continue;
            }

            if (groupInfo.status() == RaftGroupStatus.DESTROYING) {
                if (existingGroupInfo == null) {
                    createRaftGroup(groupInfo);
                } else {
                    existingGroupInfo.setDestroying();
                }
                continue;
            }

            if (groupInfo.status() == RaftGroupStatus.DESTROYED) {
                if (existingGroupInfo == null) {
                    addGroupInfo(groupInfo);
                } else {
                    completeDestroy(existingGroupInfo);
                }
                continue;
            }
        }

        for (RaftEndpoint endpoint : snapshot.getEndpoints()) {
            // TODO: restore endpoints
        }

        shutdownEndpoints.clear();
        shutdownEndpoints.addAll(snapshot.getShutdownEndpoints());

        shuttingDownEndpoint = snapshot.getShuttingDownEndpoint();
    }

    public Collection<RaftEndpoint> getAllEndpoints() {
        return allEndpoints;
    }

    public RaftEndpoint getLocalEndpoint() {
        return localEndpoint;
    }

    public RaftGroupInfo getRaftGroupInfo(RaftGroupId id) {
        return raftGroups.get(id);
    }

    public RaftGroupId createRaftGroup(String serviceName, String name, Collection<RaftEndpoint> endpoints,
            int commitIndex) {
        // keep configuration on every metadata node
        RaftGroupInfo groupInfo = getRaftGroupInfoByName(name);
        if (groupInfo != null) {
            if (groupInfo.memberCount() == endpoints.size()) {
                logger.warning("Raft group " + name + " already exists. Ignoring add raft node request.");
                return groupInfo.id();
            }

            throw new IllegalStateException("Raft group " + name
                    + " already exists with different group size. Ignoring add raft node request.");
        }

        return createRaftGroup(new RaftGroupInfo(new RaftGroupId(name, commitIndex), endpoints, serviceName));
    }

    private RaftGroupId createRaftGroup(RaftGroupInfo groupInfo) {
        addGroupInfo(groupInfo);

        RaftGroupId groupId = groupInfo.id();
        if (!groupInfo.containsMember(localEndpoint)) {
            return groupId;
        }

        raftService.createRaftNode(groupInfo);
        return groupId;
    }

    private void addGroupInfo(RaftGroupInfo groupInfo) {
        RaftGroupId groupId = groupInfo.id();
        if (raftGroups.containsKey(groupId)) {
            throw new IllegalStateException(groupInfo + " already exists.");
        }
        raftGroups.put(groupId, groupInfo);
    }

    private RaftGroupInfo getRaftGroupInfoByName(String name) {
        for (RaftGroupInfo groupInfo : raftGroups.values()) {
            if (groupInfo.status() != RaftGroupStatus.DESTROYED && groupInfo.name().equals(name)) {
                return groupInfo;
            }
        }
        return null;
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

    public void completeDestroy(Set<RaftGroupId> groupIds) {
        for (RaftGroupId groupId : groupIds) {
            completeDestroy(groupId);
        }
    }

    private void completeDestroy(RaftGroupId groupId) {
        RaftGroupInfo groupInfo = raftGroups.get(groupId);
        checkNotNull(groupInfo, "No raft group exists for " + groupId + " to commit destroy");

        completeDestroy(groupInfo);
    }

    private void completeDestroy(RaftGroupInfo groupInfo) {
        RaftGroupId groupId = groupInfo.id();
        if (groupInfo.setDestroyed()) {
            logger.info(groupId + " is destroyed.");
            raftService.destroyRaftNode(groupInfo);
        }
    }

    // !!! Called out side of Raft !!!
    public Collection<RaftGroupId> getDestroyingRaftGroupIds() {
        Collection<RaftGroupId> groupIds = new ArrayList<RaftGroupId>();
        for (RaftGroupInfo groupInfo : raftGroups.values()) {
            if (groupInfo.status() == RaftGroupStatus.DESTROYING) {
                groupIds.add(groupInfo.id());
            }
        }
        return groupIds;
    }

    public void triggerShutdownEndpoint(RaftEndpoint endpoint) {
        if (!allEndpoints.contains(endpoint)) {
            throw new IllegalArgumentException(endpoint + " doesn't exist!");
        }

        if (endpoint.equals(shuttingDownEndpoint)) {
            logger.info(endpoint + " is already marked as shutting down.");
            return;
        }

        if (shuttingDownEndpoint != null) {
            throw new CannotShutdownException("Another node " + shuttingDownEndpoint + " is currently shutting down,"
                    + " cannot process shutdown request of " + endpoint);
        }

        logger.severe("Shutting down " + endpoint);
        shuttingDownEndpoint = endpoint;

        for (RaftGroupInfo groupInfo : raftGroups.values()) {
            RaftGroupId groupId = groupInfo.id();
            if (groupId.equals(METADATA_GROUP_ID)) {
                continue;
            }
            if (groupInfo.containsMember(endpoint)) {
                if (groupInfo.leavingMember() != null) {
                    // already found a substitute
                    assert endpoint.equals(groupInfo.leavingMember()) : "Leaving: " + endpoint + " -> " + groupInfo;
                    continue;
                }

                boolean foundSubstitute = false;
                for (RaftEndpoint e : allEndpoints) {
                    // TODO: not deterministic !!!
                    if (!shutdownEndpoints.contains(e) && !groupInfo.containsMember(e)) {
                        groupInfo.markSubstitutes(endpoint, e);
                        logger.fine("Substituted " + endpoint + " with " + e + " in " + groupInfo);
                        foundSubstitute = true;
                        break;
                    }
                }
                if (!foundSubstitute) {
                    logger.fine("Cannot find a substitute for " + endpoint + " in " + groupInfo);
                }
            }
        }
    }

    public void completeShutdownEndpoint(RaftEndpoint endpoint) {
        if (!allEndpoints.contains(endpoint)) {
            throw new IllegalArgumentException(endpoint + " doesn't exist!");
        }
        if (!endpoint.equals(shuttingDownEndpoint)) {
            throw new IllegalArgumentException(endpoint + " is not shutting down member! Currently "
                    + shuttingDownEndpoint + " is shutting down.");
        }

        logger.severe("Shutdown completed for " + endpoint);
        shutdownEndpoints.add(endpoint);
        shuttingDownEndpoint = null;

        for (RaftGroupInfo groupInfo : raftGroups.values()) {
            if (groupInfo.id().equals(METADATA_GROUP_ID)) {
                continue;
            }
            if (groupInfo.containsMember(endpoint)) {
                RaftEndpoint joiningMember = groupInfo.joiningMember();
                groupInfo.completeSubstitution(endpoint);
                logger.fine("Removed " + endpoint + " from " + groupInfo);
                if (localEndpoint.equals(joiningMember)) {
                    // we are the added member to the group,
                    // create local raft node
                    raftService.createRaftNode(groupInfo);
                }
            }
        }
    }

    // Called outside of Raft
    public RaftEndpoint getShuttingDownEndpoint() {
        return shuttingDownEndpoint;
    }

    // Called outside of Raft
    public boolean isShutdown(RaftEndpoint endpoint) {
        return shutdownEndpoints.contains(endpoint);
    }

    // Called outside of Raft
    public Map<RaftGroupId, RaftEndpoint> getSubstitutesFor(RaftEndpoint endpoint) {
        Map<RaftGroupId, RaftEndpoint> substitutes = new HashMap<RaftGroupId, RaftEndpoint>();
        for (RaftGroupInfo groupInfo : raftGroups.values()) {
            if (endpoint.equals(groupInfo.leavingMember())) {
                substitutes.put(groupInfo.id(), groupInfo.joiningMember());
            }
        }
        return substitutes;
    }
}
