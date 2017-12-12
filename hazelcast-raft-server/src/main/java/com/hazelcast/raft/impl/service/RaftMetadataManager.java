package com.hazelcast.raft.impl.service;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.service.RaftGroupInfo.RaftGroupStatus;
import com.hazelcast.raft.impl.service.exception.CannotRemoveEndpointException;
import com.hazelcast.spi.NodeEngine;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.raft.impl.RaftEndpoint.parseEndpoints;
import static com.hazelcast.raft.impl.service.LeavingRaftEndpointContext.RaftGroupLeavingEndpointContext;
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
    private final Collection<RaftEndpoint> removedEndpoints = newSetFromMap(new ConcurrentHashMap<RaftEndpoint, Boolean>());
    // read outside of Raft
    private volatile LeavingRaftEndpointContext leavingEndpointContext;

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
        for (RaftEndpoint endpoint : removedEndpoints) {
            snapshot.addShutdownEndpoint(endpoint);
        }

        // TODO fixit
//        snapshot.setShuttingDownEndpoint(shuttingDownEndpoint);
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

        removedEndpoints.clear();
        removedEndpoints.addAll(snapshot.getShutdownEndpoints());

        // TODO fixit
//        shuttingDownEndpoint = snapshot.getShuttingDownEndpoint();
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

    public void triggerRemoveEndpoint(RaftEndpoint endpoint) {
        if (!allEndpoints.contains(endpoint)) {
            throw new IllegalArgumentException(endpoint + " doesn't exist!");
        }
        if (leavingEndpointContext != null) {
            if (leavingEndpointContext.getEndpoint().equals(endpoint)) {
                logger.info(endpoint + " is already marked as leaving.");
                return;
            }

            throw new CannotRemoveEndpointException("Another node " + leavingEndpointContext.getEndpoint()
                    + " is currently leaving, cannot process remove request of " + endpoint);
        }

        logger.severe("Removing " + endpoint);

        Map<RaftGroupId, RaftGroupLeavingEndpointContext> leavingGroups = new HashMap<RaftGroupId, RaftGroupLeavingEndpointContext>();
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
                for (RaftEndpoint substitute : allEndpoints) {
                    // TODO: not deterministic !!!
                    if (!removedEndpoints.contains(substitute) && !groupInfo.containsMember(substitute)) {
                        groupInfo.markSubstitutes(endpoint, substitute);
                        leavingGroups.put(groupId, new RaftGroupLeavingEndpointContext(groupInfo.getMembersCommitIndex(),
                                groupInfo.members(), substitute));
                        logger.fine("Substituted " + endpoint + " with " + substitute + " in " + groupInfo);
                        foundSubstitute = true;
                        break;
                    }
                }
                if (!foundSubstitute) {
                    logger.fine("Cannot find a substitute for " + endpoint + " in " + groupInfo);
                }
            }
        }

        leavingEndpointContext = new LeavingRaftEndpointContext(endpoint, leavingGroups);
    }

    public void completeRemoveEndpoint(RaftEndpoint endpoint, Map<RaftGroupId, Entry<Integer, Integer>> leftGroups) {
        if (!allEndpoints.contains(endpoint)) {
            throw new IllegalArgumentException(endpoint + " doesn't exist!");
        }

        if (leavingEndpointContext == null) {
            throw new IllegalStateException("There is no leaving endpoint!");
        }

        if (!leavingEndpointContext.getEndpoint().equals(endpoint)) {
            throw new IllegalArgumentException(endpoint + " is not the already leaving member! Currently "
                    + leavingEndpointContext.getEndpoint() + " is leaving.");
        }

        for (Entry<RaftGroupId, Entry<Integer, Integer>> e : leftGroups.entrySet()) {
            RaftGroupId groupId = e.getKey();
            RaftGroupInfo groupInfo = raftGroups.get(groupId);
            int expectedMembersCommitIndex = e.getValue().getKey();
            if (groupInfo.getMembersCommitIndex() == expectedMembersCommitIndex) {
                RaftEndpoint joiningMember = groupInfo.joiningMember();
                int newMembersCommitIndex = e.getValue().getValue();
                groupInfo.completeSubstitution(endpoint, newMembersCommitIndex);
                logger.fine("Removed " + endpoint + " from " + groupInfo + " with new members commit index: "
                        + newMembersCommitIndex);
                if (localEndpoint.equals(joiningMember)) {
                    // we are the added member to the group,
                    // create local raft node
                    raftService.createRaftNode(groupInfo);
                    // TODO might be already created...
                }
            }
        }

        boolean safeToRemove = true;
        for (RaftGroupInfo groupInfo : raftGroups.values()) {
            if (groupInfo.id().equals(METADATA_GROUP_ID)) {
                continue;
            }

            if (groupInfo.containsMember(endpoint)) {
                safeToRemove = false;
                break;
            }
        }

        if (safeToRemove) {
            logger.severe("Remove member completed for " + endpoint);
            removedEndpoints.add(endpoint);
            leavingEndpointContext = null;
        } else {
            // TODO shrink the groups in leavingEndpointContext
        }
    }

    // Called outside of Raft
    public LeavingRaftEndpointContext getLeavingEndpointContext() {
        return leavingEndpointContext;
    }

    // Called outside of Raft
    public boolean isRemoved(RaftEndpoint endpoint) {
        return removedEndpoints.contains(endpoint);
    }

}
