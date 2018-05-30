package com.hazelcast.raft.impl.service;

import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.config.raft.RaftMetadataGroupConfig;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberSelector;
import com.hazelcast.internal.cluster.impl.operations.MemberAttributeChangedOp;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.RaftEndpointImpl;
import com.hazelcast.raft.impl.RaftGroupIdImpl;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.service.RaftGroupInfo.RaftGroupStatus;
import com.hazelcast.raft.impl.service.exception.CannotCreateRaftGroupException;
import com.hazelcast.raft.impl.service.exception.CannotRemoveEndpointException;
import com.hazelcast.raft.impl.service.operation.metadata.CreateRaftNodeOp;
import com.hazelcast.raft.impl.service.operation.metadata.SendActiveEndpointsOp;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.cluster.memberselector.MemberSelectors.NON_LOCAL_MEMBER_SELECTOR;
import static com.hazelcast.config.raft.RaftMetadataGroupConfig.RAFT_MEMBER_ATTRIBUTE_NAME;
import static com.hazelcast.raft.impl.service.LeavingRaftEndpointContext.RaftGroupLeavingEndpointContext;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkState;
import static java.util.Collections.unmodifiableCollection;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftMetadataManager implements SnapshotAwareService<MetadataSnapshot>  {

    static final RaftGroupId METADATA_GROUP_ID = new RaftGroupIdImpl("METADATA", 0);
    private static final RaftMemberSelector RAFT_MEMBER_SELECTOR = new RaftMemberSelector();

    private final NodeEngine nodeEngine;
    private final RaftService raftService;
    private final ILogger logger;
    private final RaftMetadataGroupConfig config;

    private final AtomicReference<RaftEndpointImpl> localEndpoint = new AtomicReference<RaftEndpointImpl>();
    // groups are read outside of Raft
    private final Map<RaftGroupId, RaftGroupInfo> groups = new ConcurrentHashMap<RaftGroupId, RaftGroupInfo>();
    // activeEndpoints must be an ordered non-null collection
    private volatile Collection<RaftEndpointImpl> activeEndpoints = Collections.emptySet();
    private LeavingRaftEndpointContext leavingEndpointContext;

    RaftMetadataManager(NodeEngine nodeEngine, RaftService raftService, RaftMetadataGroupConfig config) {
        this.nodeEngine = nodeEngine;
        this.raftService = raftService;
        this.logger = nodeEngine.getLogger(getClass());
        this.config = config;
    }

    void initIfInitialRaftMember() {
        boolean initialRaftMember = config.isInitialRaftMember();
        if (!initialRaftMember) {
            logger.warning("We are not one of Raft members :(");
            return;
        }

        init();

        // task for initial Raft members
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.schedule(new DiscoverInitialRaftEndpointsTask(), 500, TimeUnit.MILLISECONDS);
    }

    void init() {
        Member localMember = nodeEngine.getLocalMember();
        if (!localEndpoint.compareAndSet(null, new RaftEndpointImpl(localMember))) {
            // already initialized
            return;
        }

        logger.info("Raft members: " + activeEndpoints + ", local: " + localEndpoint);
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.scheduleWithRepetition(new BroadcastActiveEndpointsTask(), 10, 10, TimeUnit.SECONDS);

        RaftCleanupHandler cleanupHandler = new RaftCleanupHandler(nodeEngine, raftService);
        cleanupHandler.init();
    }

    void reset() {
        activeEndpoints = Collections.emptySet();
        groups.clear();

        init();

        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.schedule(new DiscoverInitialRaftEndpointsTask(), 500, TimeUnit.MILLISECONDS);
    }

    @Override
    public MetadataSnapshot takeSnapshot(RaftGroupId groupId, long commitIndex) {
        if (!METADATA_GROUP_ID.equals(groupId)) {
            return null;
        }

        logger.info("Taking snapshot for commit-index: " + commitIndex);
        MetadataSnapshot snapshot = new MetadataSnapshot();
        for (RaftGroupInfo group : groups.values()) {
            assert group.commitIndex() <= commitIndex
                    : "Group commit index: " + group.commitIndex() + ", snapshot commit index: " + commitIndex;
            snapshot.addRaftGroup(group);
        }
        for (RaftEndpointImpl endpoint : activeEndpoints) {
            snapshot.addEndpoint(endpoint);
        }
        snapshot.setLeavingRaftEndpointContext(leavingEndpointContext);
        return snapshot;
    }

    @Override
    public void restoreSnapshot(RaftGroupId groupId, long commitIndex, MetadataSnapshot snapshot) {
        ensureMetadataGroupId(groupId);

        logger.info("Restoring snapshot for commit-index: " + commitIndex);
        for (RaftGroupInfo group : snapshot.getRaftGroups()) {
            RaftGroupInfo existingGroup = groups.get(group.id());

            if (group.status() == RaftGroupStatus.ACTIVE && existingGroup == null) {
                createRaftGroup(group);
                continue;
            }

            if (group.status() == RaftGroupStatus.DESTROYING) {
                if (existingGroup == null) {
                    createRaftGroup(group);
                } else {
                    existingGroup.setDestroying();
                }
                continue;
            }

            if (group.status() == RaftGroupStatus.DESTROYED) {
                if (existingGroup == null) {
                    addRaftGroup(group);
                } else {
                    completeDestroyRaftGroup(existingGroup);
                }
            }
        }

        activeEndpoints = unmodifiableCollection(new LinkedHashSet<RaftEndpointImpl>(snapshot.getEndpoints()));
        leavingEndpointContext = snapshot.getLeavingRaftEndpointContext();

        updateInvocationManagerEndpoints(getActiveEndpoints());
    }

    private static void ensureMetadataGroupId(RaftGroupId groupId) {
        if (!METADATA_GROUP_ID.equals(groupId)) {
            throw new IllegalArgumentException("Invalid RaftGroupId! Expected: " + METADATA_GROUP_ID
                    + ", Actual: " + groupId);
        }
    }

    public RaftEndpointImpl getLocalEndpoint() {
        return localEndpoint.get();
    }

    public RaftGroupInfo getRaftGroup(RaftGroupId groupId) {
        return groups.get(groupId);
    }

    public RaftGroupId createRaftGroup(String groupName, Collection<RaftEndpointImpl> endpoints, long commitIndex) {
        // keep configuration on every metadata node
        RaftGroupInfo group = getRaftGroupByName(groupName);
        if (group != null) {
            if (group.memberCount() == endpoints.size()) {
                logger.warning("Raft group " + groupName + " already exists. Ignoring add raft node request.");
                return group.id();
            }

            throw new IllegalStateException("Raft group " + groupName
                    + " already exists with different group size. Ignoring add raft node request.");
        }

        RaftEndpointImpl leavingEndpoint = leavingEndpointContext != null ? leavingEndpointContext.getEndpoint() : null;
        for (RaftEndpointImpl endpoint : endpoints) {
            if (endpoint.equals(leavingEndpoint) || !activeEndpoints.contains(endpoint)) {
                throw new CannotCreateRaftGroupException("Cannot create raft group: " + groupName + " since " + endpoint
                        + " is not active");
            }
        }

        return createRaftGroup(new RaftGroupInfo(new RaftGroupIdImpl(groupName, commitIndex), endpoints));
    }

    private RaftGroupId createRaftGroup(RaftGroupInfo group) {
        addRaftGroup(group);
        logger.info("New raft group: " + group.id() + " is created with members: " + group.members());

        RaftGroupId groupId = group.id();
        if (group.containsMember(localEndpoint.get())) {
            raftService.createRaftNode(groupId, group.members());
        } else {
            // Broadcast group-info to non-metadata group members
            OperationService operationService = nodeEngine.getOperationService();
            RaftGroupInfo metadataGroup = groups.get(RaftService.METADATA_GROUP_ID);
            for (RaftEndpointImpl endpoint : group.endpointImpls()) {
                if (!metadataGroup.containsMember(endpoint)) {
                    operationService.send(new CreateRaftNodeOp(group), endpoint.getAddress());
                }
            }
        }

        return groupId;
    }

    private void addRaftGroup(RaftGroupInfo group) {
        RaftGroupId groupId = group.id();
        checkState(!groups.containsKey(groupId), group + " already exists!" );
        groups.put(groupId, group);
    }

    private RaftGroupInfo getRaftGroupByName(String name) {
        for (RaftGroupInfo group : groups.values()) {
            if (group.status() != RaftGroupStatus.DESTROYED && group.name().equals(name)) {
                return group;
            }
        }
        return null;
    }

    public void triggerDestroyRaftGroup(RaftGroupId groupId) {
        RaftGroupInfo group = groups.get(groupId);
        checkNotNull(group, "No raft group exists for " + groupId + " to trigger destroy");

        if (group.setDestroying()) {
            logger.info("Destroying " + groupId);
        } else {
            logger.info(groupId + " is already " + group.status());
        }
    }

    public void completeDestroyRaftGroups(Set<RaftGroupId> groupIds) {
        for (RaftGroupId groupId : groupIds) {
            completeDestroyRaftGroup(groupId);
        }
    }

    private void completeDestroyRaftGroup(RaftGroupId groupId) {
        RaftGroupInfo group = groups.get(groupId);
        checkNotNull(group, "No raft group exists for " + groupId + " to commit destroy");

        completeDestroyRaftGroup(group);
    }

    private void completeDestroyRaftGroup(RaftGroupInfo group) {
        RaftGroupId groupId = group.id();
        if (group.setDestroyed()) {
            logger.info(groupId + " is destroyed.");
            raftService.destroyRaftNode(groupId);
        }
    }

    /**
     * this method is idempotent
     */
    public void triggerRemoveEndpoint(RaftEndpointImpl leavingEndpoint) {
        if (!activeEndpoints.contains(leavingEndpoint)) {
            logger.warning("Not removing " + leavingEndpoint + " since it is not present in the active endpoints");
            return;
        }

        if (leavingEndpointContext != null) {
            if (leavingEndpointContext.getEndpoint().equals(leavingEndpoint)) {
                logger.info(leavingEndpoint + " is already marked as leaving.");
                return;
            }

            throw new CannotRemoveEndpointException("Another node " + leavingEndpointContext.getEndpoint()
                    + " is currently leaving, cannot process remove request of " + leavingEndpoint);
        }

        logger.info("Removing " + leavingEndpoint + " from raft groups");

        if (activeEndpoints.size() <= 2) {
            logger.warning(leavingEndpoint + " is directly removed as there are only " + activeEndpoints.size() + " endpoints");
            removeActiveEndpoint(leavingEndpoint);
            return;
        }

        Map<RaftGroupId, RaftGroupLeavingEndpointContext> leavingGroups = new LinkedHashMap<RaftGroupId, RaftGroupLeavingEndpointContext>();
        for (RaftGroupInfo group : groups.values()) {
            RaftGroupId groupId = group.id();
            if (group.containsMember(leavingEndpoint)) {
                boolean foundSubstitute = false;
                for (RaftEndpointImpl substitute : activeEndpoints) {
                    if (activeEndpoints.contains(substitute) && !group.containsMember(substitute)) {
                        leavingGroups.put(groupId, new RaftGroupLeavingEndpointContext(group.getMembersCommitIndex(),
                                group.endpointImpls(), substitute));
                        logger.fine("Substituted " + leavingEndpoint + " with " + substitute + " in " + group);
                        foundSubstitute = true;
                        break;
                    }
                }
                if (!foundSubstitute) {
                    logger.fine("Cannot find a substitute for " + leavingEndpoint + " in " + group);
                    leavingGroups.put(groupId, new RaftGroupLeavingEndpointContext(group.getMembersCommitIndex(),
                            group.endpointImpls(), null));
                }
            }
        }

        leavingEndpointContext = new LeavingRaftEndpointContext(leavingEndpoint, leavingGroups);
    }

    public void completeRemoveEndpoint(RaftEndpointImpl leavingEndpoint, Map<RaftGroupId, Tuple2<Long, Long>> leftGroups) {
        if (!activeEndpoints.contains(leavingEndpoint)) {
            throw new IllegalArgumentException("Cannot remove " + leavingEndpoint + " from groups: " + leftGroups.keySet()
                    + " since " +  leavingEndpoint + " doesn't exist!");
        }

        if (leavingEndpointContext == null) {
            throw new IllegalStateException("Cannot remove " + leavingEndpoint + " from groups: " + leftGroups.keySet()
                    + " since there is no leaving endpoint!");
        }

        if (!leavingEndpointContext.getEndpoint().equals(leavingEndpoint)) {
            throw new IllegalArgumentException("Cannot remove " + leavingEndpoint + " from groups: " + leftGroups.keySet()
                    + " since " + leavingEndpointContext.getEndpoint() + " is currently leaving.");
        }

        Map<RaftGroupId, RaftGroupLeavingEndpointContext> leavingGroups = leavingEndpointContext.getGroups();
        for (Entry<RaftGroupId, Tuple2<Long, Long>> e : leftGroups.entrySet()) {
            RaftGroupId groupId = e.getKey();
            RaftGroupInfo group = groups.get(groupId);

            Tuple2<Long, Long> value = e.getValue();
            long expectedMembersCommitIndex = value.element1;
            long newMembersCommitIndex = value.element2;
            RaftEndpointImpl joining = leavingGroups.get(groupId).getSubstitute();

            if (group.substitute(leavingEndpoint, joining, expectedMembersCommitIndex, newMembersCommitIndex)) {
                logger.fine("Removed " + leavingEndpoint + " from " + group + " with new members commit index: "
                        + newMembersCommitIndex);
                if (localEndpoint.get().equals(joining)) {
                    // we are the added member to the group, we can try to create the local raft node if not created already
                    raftService.createRaftNode(groupId, group.members());
                } else if (joining != null) {
                    // publish group-info to the joining member
                    nodeEngine.getOperationService().send(new CreateRaftNodeOp(group), joining.getAddress());
                }
            } else {
                logger.warning("Could not substitute " + leavingEndpoint + " with " + joining + " in " + groupId);
            }
        }

        boolean safeToRemove = true;
        for (RaftGroupInfo group : groups.values()) {
            if (group.containsMember(leavingEndpoint)) {
                safeToRemove = false;
                break;
            }
        }

        if (safeToRemove) {
            logger.info("Remove member procedure completed for " + leavingEndpoint);
            removeActiveEndpoint(leavingEndpoint);
            leavingEndpointContext = null;
        } else if (!leftGroups.isEmpty()) {
            // no need to re-attempt for successfully left groups
            leavingEndpointContext = leavingEndpointContext.exclude(leftGroups.keySet());
        }
    }

    public boolean isEndpointRemoved(RaftEndpointImpl endpoint) {
        return !activeEndpoints.contains(endpoint);
    }

    public Collection<RaftEndpointImpl> getActiveEndpoints() {
        if (leavingEndpointContext == null) {
            return activeEndpoints;
        }
        List<RaftEndpointImpl> active = new ArrayList<RaftEndpointImpl>(activeEndpoints);
        active.remove(leavingEndpointContext.getEndpoint());
        return active;
    }

    public void setActiveEndpoints(Collection<RaftEndpointImpl> endpoints) {
        if (localEndpoint.get() != null) {
            throw new IllegalStateException("This node is already part of Raft members!");
        }
        logger.fine("Setting active endpoints to " + endpoints);
        activeEndpoints = unmodifiableCollection(new LinkedHashSet<RaftEndpointImpl>(endpoints));
        updateInvocationManagerEndpoints(endpoints);
    }

    private void updateInvocationManagerEndpoints(Collection<RaftEndpointImpl> endpoints) {
        raftService.getInvocationManager().setAllEndpoints(endpoints);
    }

    public Collection<RaftGroupId> getDestroyingRaftGroupIds() {
        Collection<RaftGroupId> groupIds = new ArrayList<RaftGroupId>();
        for (RaftGroupInfo group : groups.values()) {
            if (group.status() == RaftGroupStatus.DESTROYING) {
                groupIds.add(group.id());
            }
        }
        return groupIds;
    }

    public LeavingRaftEndpointContext getLeavingEndpointContext() {
        return leavingEndpointContext;
    }

    boolean isMetadataLeader() {
        RaftEndpointImpl endpoint = localEndpoint.get();
        if (endpoint == null) {
            return false;
        }
        RaftNode raftNode = raftService.getRaftNode(RaftService.METADATA_GROUP_ID);
        // even if the local leader information is stale, it is fine.
        return raftNode != null && !raftNode.isTerminatedOrSteppedDown() && endpoint.equals(raftNode.getLeader());
    }

    /**
     * this method is idempotent
     */
    public void addActiveEndpoint(RaftEndpointImpl endpoint) {
        if (activeEndpoints.contains(endpoint)) {
            logger.fine(endpoint + " already exists. Silently returning from addActiveEndpoint().");
            return;
        }
        if (leavingEndpointContext != null && endpoint.equals(leavingEndpointContext.getEndpoint())) {
            throw new IllegalArgumentException(endpoint + " is already being removed!");
        }
        Collection<RaftEndpointImpl> newEndpoints = new LinkedHashSet<RaftEndpointImpl>(activeEndpoints);
        newEndpoints.add(endpoint);
        activeEndpoints = unmodifiableCollection(newEndpoints);
        updateInvocationManagerEndpoints(newEndpoints);
        broadcastActiveEndpoints();
        logger.info("Added " + endpoint + ". Active endpoints are " + newEndpoints);
    }

    private void removeActiveEndpoint(RaftEndpointImpl endpoint) {
        Collection<RaftEndpointImpl> newEndpoints = new LinkedHashSet<RaftEndpointImpl>(activeEndpoints);
        newEndpoints.remove(endpoint);
        activeEndpoints = unmodifiableCollection(newEndpoints);
        updateInvocationManagerEndpoints(newEndpoints);
        broadcastActiveEndpoints();
    }

    private class BroadcastActiveEndpointsTask implements Runnable {
        @Override
        public void run() {
            if (!isMetadataLeader()) {
                return;
            }
            broadcastActiveEndpoints();
        }
    }

    void broadcastActiveEndpoints() {
        if (localEndpoint.get() == null) {
            return;
        }
        Collection<RaftEndpointImpl> endpoints = activeEndpoints;
        if (endpoints.isEmpty()) {
            return;
        }

        Set<Address> addresses = new HashSet<Address>(endpoints.size());
        for (RaftEndpointImpl endpoint : endpoints) {
            addresses.add(endpoint.getAddress());
        }

        Set<Member> members = nodeEngine.getClusterService().getMembers();
        OperationService operationService = nodeEngine.getOperationService();
        Operation op = new SendActiveEndpointsOp(getActiveEndpoints());
        for (Member member : members) {
            if (addresses.contains(member.getAddress())) {
                continue;
            }
            operationService.send(op, member.getAddress());
        }
    }

    private class DiscoverInitialRaftEndpointsTask implements Runnable {

        @Override
        public void run() {
            ExecutionService executionService = nodeEngine.getExecutionService();
            Collection<Member> members = nodeEngine.getClusterService().getMembers(RAFT_MEMBER_SELECTOR);

            setInitialRaftMemberAttribute();

            if (members.size() < config.getGroupSize()) {
                logger.warning("Waiting for " + config.getGroupSize() + " Raft members to join the cluster. "
                        + "Current Raft members count: " + members.size());
                executionService.schedule(this, 500, TimeUnit.MILLISECONDS);
                return;
            }

            if (members.size() > config.getGroupSize()) {
                logger.severe("INVALID RAFT MEMBERS INITIALIZATION !!! "
                        + "Expected Raft member count: " + config.getGroupSize()
                        + ", Current member count: " + members.size());

                List<Member> raftMembers = new ArrayList<Member>(members).subList(0, config.getGroupSize());
                if (!raftMembers.contains(nodeEngine.getLocalMember())) {
                    logger.warning("We are not a Raft member!");
                    localEndpoint.set(null);
                    return;
                }
            }

            Collection<RaftEndpointImpl> all = new LinkedHashSet<RaftEndpointImpl>(config.getGroupSize());
            List<RaftEndpointImpl> metadata = new ArrayList<RaftEndpointImpl>(config.getMetadataGroupSize());
            int index = 0;
            for (Member member : members) {
                RaftEndpointImpl endpoint = new RaftEndpointImpl(member);
                all.add(endpoint);

                if (index < config.getMetadataGroupSize()) {
                    metadata.add(endpoint);
                }

                if (++index == config.getGroupSize()) {
                    break;
                }
            }
            activeEndpoints = unmodifiableCollection(all);
            updateInvocationManagerEndpoints(all);

            if (metadata.contains(localEndpoint.get())) {
                createRaftGroup(new RaftGroupInfo(METADATA_GROUP_ID, sort(metadata)));
            }

            broadcastActiveEndpoints();
        }

        private void setInitialRaftMemberAttribute() {
            Member localMember = nodeEngine.getLocalMember();
            if (Boolean.TRUE.equals(localMember.getBooleanAttribute(RAFT_MEMBER_ATTRIBUTE_NAME))) {
                // Member attributes have a weak replication guarantee.
                // Broadcast member attribute to the cluster again to make sure everyone learns it eventually.
                Operation op = new MemberAttributeChangedOp(MemberAttributeOperationType.PUT, RAFT_MEMBER_ATTRIBUTE_NAME, true);
                for (Member member : nodeEngine.getClusterService().getMembers(NON_LOCAL_MEMBER_SELECTOR)) {
                    nodeEngine.getOperationService().send(op, member.getAddress());
                }
            } else {
                localMember.setBooleanAttribute(RAFT_MEMBER_ATTRIBUTE_NAME, true);
            }
        }
    }

    private static List<RaftEndpointImpl> sort(List<RaftEndpointImpl> endpoints) {
        Collections.sort(endpoints, new Comparator<RaftEndpointImpl>() {
            @Override
            public int compare(RaftEndpointImpl e1, RaftEndpointImpl e2) {
                return e1.getUid().compareTo(e2.getUid());
            }
        });

        return endpoints;
    }

    private static class RaftMemberSelector implements MemberSelector {
        @Override
        public boolean select(Member member) {
            Boolean raftMember = member.getBooleanAttribute(RAFT_MEMBER_ATTRIBUTE_NAME);
            return Boolean.TRUE.equals(raftMember);
        }
    }
}
