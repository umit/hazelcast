/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cp.internal;

import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.Member;
import com.hazelcast.cp.CPGroup.CPGroupStatus;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.exception.CannotCreateRaftGroupException;
import com.hazelcast.cp.internal.exception.CannotRemoveCPMemberException;
import com.hazelcast.cp.internal.exception.MetadataRaftGroupNotInitializedException;
import com.hazelcast.cp.internal.raft.SnapshotAwareService;
import com.hazelcast.cp.internal.raft.impl.RaftNode;
import com.hazelcast.cp.internal.raftop.metadata.CreateMetadataRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.CreateRaftNodeOp;
import com.hazelcast.cp.internal.raftop.metadata.DestroyRaftNodesOp;
import com.hazelcast.cp.internal.raftop.metadata.SendActiveCPMembersOp;
import com.hazelcast.cp.internal.util.Tuple2;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.cp.CPGroup.CPGroupStatus.ACTIVE;
import static com.hazelcast.cp.CPGroup.CPGroupStatus.DESTROYED;
import static com.hazelcast.cp.CPGroup.CPGroupStatus.DESTROYING;
import static com.hazelcast.cp.internal.MembershipChangeContext.CPGroupMembershipChangeContext;
import static com.hazelcast.util.Preconditions.checkFalse;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkState;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.util.Collections.singleton;
import static java.util.Collections.sort;
import static java.util.Collections.unmodifiableCollection;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Maintains the CP subsystem metadata, such as CP groups, active CP members,
 * leaving and joining CP members, etc.
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classdataabstractioncoupling"})
public class MetadataRaftGroupManager implements SnapshotAwareService<MetadataRaftGroupSnapshot>  {

    public static final CPGroupId METADATA_GROUP_ID = new RaftGroupId("METADATA", 0);

    private static final long DISCOVER_INITIAL_CP_MEMBERS_TASK_DELAY_MILLIS = 500;
    private static final long BROADCAST_ACTIVE_CP_MEMBERS_TASK_PERIOD_SECONDS = 10;

    private final NodeEngine nodeEngine;
    private final RaftService raftService;
    private final ILogger logger;
    private final CPSubsystemConfig config;

    private final AtomicReference<CPMember> localMember = new AtomicReference<CPMember>();
    // groups are read outside of Raft
    private final ConcurrentMap<CPGroupId, RaftGroup> groups = new ConcurrentHashMap<CPGroupId, RaftGroup>();
    // activeMembers must be an ordered non-null collection
    private volatile Collection<CPMember> activeMembers = Collections.emptySet();
    private final AtomicBoolean discoveryCompleted = new AtomicBoolean();
    private Collection<CPMember> initialCPMembers;
    private MembershipChangeContext membershipChangeContext;

    MetadataRaftGroupManager(NodeEngine nodeEngine, RaftService raftService, CPSubsystemConfig config) {
        this.nodeEngine = nodeEngine;
        this.raftService = raftService;
        this.logger = nodeEngine.getLogger(getClass());
        this.config = config;
    }

    void initLocalCPMemberOnStartup() {
        initLocalMember();

        // task for initial CP members
        nodeEngine.getExecutionService()
                  .schedule(new DiscoverInitialCPMembersTask(), DISCOVER_INITIAL_CP_MEMBERS_TASK_DELAY_MILLIS, MILLISECONDS);
    }

    void initPromotedCPMember() {
        if (!initLocalMember()) {
            return;
        }

        scheduleRaftGroupMembershipManagementTasks();
    }

    private void scheduleRaftGroupMembershipManagementTasks() {
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.scheduleWithRepetition(new BroadcastActiveCPMembersTask(),
                BROADCAST_ACTIVE_CP_MEMBERS_TASK_PERIOD_SECONDS, BROADCAST_ACTIVE_CP_MEMBERS_TASK_PERIOD_SECONDS, SECONDS);

        RaftGroupMembershipManager membershipManager = new RaftGroupMembershipManager(nodeEngine, raftService);
        membershipManager.init();
    }

    boolean initLocalMember() {
        Member localMember = nodeEngine.getLocalMember();
        return this.localMember.compareAndSet(null, new CPMember(localMember));
    }

    void reset() {
        doSetActiveMembers(Collections.<CPMember>emptySet());
        groups.clear();
        initialCPMembers = null;

        if (config == null) {
            return;
        }
        discoveryCompleted.set(false);
        initLocalMember();

        nodeEngine.getExecutionService()
                  .schedule(new DiscoverInitialCPMembersTask(), DISCOVER_INITIAL_CP_MEMBERS_TASK_DELAY_MILLIS, MILLISECONDS);
    }

    @Override
    public MetadataRaftGroupSnapshot takeSnapshot(CPGroupId groupId, long commitIndex) {
        if (!METADATA_GROUP_ID.equals(groupId)) {
            return null;
        }

        logger.info("Taking snapshot for commit-index: " + commitIndex);
        MetadataRaftGroupSnapshot snapshot = new MetadataRaftGroupSnapshot();
        for (RaftGroup group : groups.values()) {
            assert group.commitIndex() <= commitIndex
                    : "Group commit index: " + group.commitIndex() + ", snapshot commit index: " + commitIndex;
            snapshot.addRaftGroup(group);
        }
        for (CPMember member : activeMembers) {
            snapshot.addMember(member);
        }
        snapshot.setMembershipChangeContext(membershipChangeContext);
        return snapshot;
    }

    @Override
    public void restoreSnapshot(CPGroupId groupId, long commitIndex, MetadataRaftGroupSnapshot snapshot) {
        ensureMetadataGroupId(groupId);
        checkNotNull(snapshot);

        logger.info("Restoring snapshot for commit-index: " + commitIndex);
        for (RaftGroup group : snapshot.getRaftGroups()) {
            RaftGroup existingGroup = groups.get(group.id());

            if (group.status() == ACTIVE && existingGroup == null) {
                createRaftGroup(group);
                continue;
            }

            if (group.status() == DESTROYING) {
                if (existingGroup == null) {
                    createRaftGroup(group);
                } else {
                    existingGroup.setDestroying();
                }
                continue;
            }

            if (group.status() == DESTROYED) {
                if (existingGroup == null) {
                    addRaftGroup(group);
                } else {
                    completeDestroyRaftGroup(existingGroup);
                }
            }
        }

        doSetActiveMembers(unmodifiableCollection(new LinkedHashSet<CPMember>(snapshot.getMembers())));

        membershipChangeContext = snapshot.getMembershipChangeContext();
    }

    private static void ensureMetadataGroupId(CPGroupId groupId) {
        checkTrue(METADATA_GROUP_ID.equals(groupId), "Invalid RaftGroupId! Expected: " + METADATA_GROUP_ID
                + ", Actual: " + groupId);
    }

    CPMember getLocalMember() {
        return localMember.get();
    }

    public Collection<CPGroupId> getGroupIds() {
        return new ArrayList<CPGroupId>(groups.keySet());
    }

    public RaftGroup getRaftGroup(CPGroupId groupId) {
        checkNotNull(groupId);

        return groups.get(groupId);
    }

    public CPGroupId getActiveRaftGroupId(String groupName) {
        for (RaftGroup group : groups.values()) {
            if (group.status() == CPGroupStatus.ACTIVE && group.name().equals(groupName)) {
                return group.id();
            }
        }

        return null;
    }

    public void createInitialMetadataRaftGroup(List<CPMember> initialMembers, int metadataMembersCount) {
        checkNotNull(initialMembers);
        checkTrue(metadataMembersCount > 1, "initial METADATA CP group must contain at least 2 members: "
                + metadataMembersCount);
        checkTrue(initialMembers.size() >= metadataMembersCount, "Initial CP members should contain all metadata members");

        if (initialCPMembers != null) {
            checkTrue(initialCPMembers.size() == initialMembers.size(), "Invalid initial CP members! Expected: "
                    + initialCPMembers + ", Actual: " + initialMembers);
            checkTrue(initialCPMembers.containsAll(initialMembers), "Invalid initial CP members! Expected: "
                    + initialCPMembers + ", Actual: " + initialMembers);
        }

        List<CPMember> metadataMembers = initialMembers.subList(0, metadataMembersCount);
        RaftGroup metadataGroup = new RaftGroup(METADATA_GROUP_ID, metadataMembers);
        RaftGroup existingMetadataGroup = groups.putIfAbsent(METADATA_GROUP_ID, metadataGroup);
        if (existingMetadataGroup != null) {
            checkTrue(metadataMembersCount == existingMetadataGroup.initialMemberCount(),
                    "Cannot create METADATA CP group with " + metadataMembersCount
                            + " because it already exists with a different member list: " + existingMetadataGroup);

            for (CPMember member : metadataMembers) {
                checkTrue(existingMetadataGroup.containsInitialMember(member),
                        "Cannot create METADATA CP group with " + metadataMembersCount
                        + " because it already exists with a different member list: " + existingMetadataGroup);
            }

            return;
        }

        initialCPMembers = initialMembers;

        logger.fine("METADATA CP group is created: " + metadataGroup);
    }

    public CPGroupId createRaftGroup(String groupName, Collection<CPMember> members, long commitIndex) {
        checkFalse(METADATA_GROUP_ID.name().equals(groupName), groupName + " is reserved for internal usage!");
        checkIfMetadataRaftGroupInitialized();

        // keep configuration on every metadata node
        RaftGroup group = getRaftGroupByName(groupName);
        if (group != null) {
            if (group.memberCount() == members.size()) {
                logger.warning("CP group " + groupName + " already exists.");
                return group.id();
            }

            throw new IllegalStateException("CP group " + groupName + " already exists with different group size.");
        }

        CPMember leavingMember = membershipChangeContext != null ? membershipChangeContext.getLeavingMember() : null;
        for (CPMember member : members) {
            if (member.equals(leavingMember) || !activeMembers.contains(member)) {
                throw new CannotCreateRaftGroupException("Cannot create CP group: " + groupName + " since " + member
                        + " is not active");
            }
        }

        return createRaftGroup(new RaftGroup(new RaftGroupId(groupName, commitIndex), members));
    }

    private CPGroupId createRaftGroup(RaftGroup group) {
        addRaftGroup(group);
        logger.info("New CP group: " + group.id() + " is created with members: " + group.members());

        CPGroupId groupId = group.id();
        if (group.containsMember(getLocalMember())) {
            raftService.createRaftNode(groupId, group.members());
        } else {
            // Broadcast group-info to non-metadata group members
            OperationService operationService = nodeEngine.getOperationService();
            RaftGroup metadataGroup = groups.get(METADATA_GROUP_ID);
            for (CPMember member : group.memberImpls()) {
                if (!metadataGroup.containsMember(member)) {
                    operationService.send(new CreateRaftNodeOp(group.id(), group.initialMembers()), member.getAddress());
                }
            }
        }

        return groupId;
    }

    private void addRaftGroup(RaftGroup group) {
        CPGroupId groupId = group.id();
        checkState(!groups.containsKey(groupId), group + " already exists!");
        groups.put(groupId, group);
    }

    private RaftGroup getRaftGroupByName(String name) {
        for (RaftGroup group : groups.values()) {
            if (group.status() != DESTROYED && group.name().equals(name)) {
                return group;
            }
        }
        return null;
    }

    public void triggerDestroyRaftGroup(CPGroupId groupId) {
        checkNotNull(groupId);
        checkState(membershipChangeContext == null,
                "Cannot destroy CP group while there are ongoing CP group membership changes!");

        RaftGroup group = groups.get(groupId);
        checkNotNull(group, "No CP group exists for " + groupId + " to destroy!");

        if (group.setDestroying()) {
            logger.info("Destroying " + groupId);
        } else {
            logger.info(groupId + " is already " + group.status());
        }
    }

    public void completeDestroyRaftGroups(Set<CPGroupId> groupIds) {
        checkNotNull(groupIds);

        for (CPGroupId groupId : groupIds) {
            checkNotNull(groupId);

            RaftGroup group = groups.get(groupId);
            checkNotNull(group, "No CP group exists for " + groupId + " to complete destroy");

            completeDestroyRaftGroup(group);
        }
    }

    private void completeDestroyRaftGroup(RaftGroup group) {
        CPGroupId groupId = group.id();
        if (group.setDestroyed()) {
            logger.info(groupId + " is destroyed.");
            sendDestroyRaftNodeOps(group);
        } else {
            logger.fine(groupId + " is already destroyed.");
        }
    }

    public void forceDestroyRaftGroup(CPGroupId groupId) {
        checkNotNull(groupId);
        checkFalse(METADATA_GROUP_ID.equals(groupId), "Cannot force-destroy the METADATA CP group!");

        RaftGroup group = groups.get(groupId);
        checkNotNull(group, "No CP group exists for " + groupId + " to force-destroy");

        if (group.forceSetDestroyed()) {
            logger.info(groupId + " is force-destroyed.");
            sendDestroyRaftNodeOps(group);
        } else {
            logger.fine(groupId + " is already force-destroyed.");
        }
    }

    private void sendDestroyRaftNodeOps(RaftGroup group) {
        OperationService operationService = nodeEngine.getOperationService();
        Operation op = new DestroyRaftNodesOp(singleton(group.id()));
        for (CPMember member : group.memberImpls())  {
            if (member.equals(getLocalMember())) {
                raftService.destroyRaftNode(group.id());
            } else {
                operationService.send(op, member.getAddress());
            }
        }
    }

    /**
     * this method is idempotent
     */
    public void triggerRemoveMember(CPMember leavingMember) {
        checkNotNull(leavingMember);
        checkIfMetadataRaftGroupInitialized();

        if (shouldRemoveMember(leavingMember)) {
            return;
        }

        if (activeMembers.size() <= 2) {
            logger.warning(leavingMember + " is directly removed as there are only " + activeMembers.size() + " members");
            removeActiveMember(leavingMember);
            return;
        }

        List<CPGroupMembershipChangeContext> leavingGroups = new ArrayList<CPGroupMembershipChangeContext>();
        for (RaftGroup group : groups.values()) {
            CPGroupId groupId = group.id();
            if (!group.containsMember(leavingMember) || group.status() == DESTROYED) {
                continue;
            }

            CPMember substitute = findSubstitute(group);
            if (substitute != null) {
                leavingGroups.add(new CPGroupMembershipChangeContext(group.id(), group.getMembersCommitIndex(),
                        group.memberImpls(), substitute, leavingMember));
                logger.fine("Substituted " + leavingMember + " with " + substitute + " in " + group);
            } else {
                logger.fine("Cannot find a substitute for " + leavingMember + " in " + group);
                leavingGroups.add(new CPGroupMembershipChangeContext(groupId, group.getMembersCommitIndex(),
                        group.memberImpls(), null, leavingMember));
            }
        }

        if (leavingGroups.isEmpty()) {
            logger.info(leavingMember + " is not present in any CP group. Removing it directly.");
            removeActiveMember(leavingMember);
            return;
        }

        membershipChangeContext = new MembershipChangeContext(leavingMember, leavingGroups);
        logger.info("Removing " + leavingMember + " from CP groups: " + leavingGroups);
    }

    private boolean shouldRemoveMember(CPMember leavingMember) {
        if (!activeMembers.contains(leavingMember)) {
            logger.warning("Not removing " + leavingMember + " since it is not present in the active members");
            return false;
        }

        if (membershipChangeContext != null) {
            if (leavingMember.equals(membershipChangeContext.getLeavingMember())) {
                logger.info(leavingMember + " is already marked as leaving.");
                return true;
            }

            throw new CannotRemoveCPMemberException("There is already an ongoing CP group membership change process. "
                    + "Cannot process remove request of " + leavingMember);
        }

        return false;
    }

    private CPMember findSubstitute(RaftGroup group) {
        for (CPMember substitute : activeMembers) {
            if (activeMembers.contains(substitute) && !group.containsMember(substitute)) {
                return substitute;
            }
        }

        return null;
    }

    public MembershipChangeContext completeRaftGroupMembershipChanges(Map<CPGroupId, Tuple2<Long, Long>> changedGroups) {
        checkNotNull(changedGroups);
        checkState(membershipChangeContext != null, "Cannot apply CP group membership changes: "
                + changedGroups + " since there is no membership change context!");

        for (CPGroupMembershipChangeContext ctx : membershipChangeContext.getChanges()) {
            CPGroupId groupId = ctx.getGroupId();
            RaftGroup group = groups.get(groupId);
            checkState(group != null, groupId + "not found in CP groups: " + groups.keySet()
                    + "to apply " + ctx);
            Tuple2<Long, Long> t = changedGroups.get(groupId);

            if (t == null) {
                if (group.status() == DESTROYED && !changedGroups.containsKey(groupId)) {
                    logger.warning(groupId + " is already destroyed so will skip: " + ctx);
                    changedGroups.put(groupId, Tuple2.of(0L, 0L));
                }
                continue;
            }

            applyMembershipChange(ctx, group, t.element1, t.element2);
        }

        membershipChangeContext = membershipChangeContext.excludeCompletedChanges(changedGroups.keySet());

        CPMember leavingMember = membershipChangeContext.getLeavingMember();
        if (checkSafeToRemove(leavingMember)) {
            checkState(membershipChangeContext.getChanges().isEmpty(), "Leaving " + leavingMember
                    + " is removed from all groups but there are still pending membership changes: "
                    + membershipChangeContext);
            logger.info(leavingMember + " is removed from all CP groups and active members");
            removeActiveMember(leavingMember);
            membershipChangeContext = null;
        } else if (membershipChangeContext.getChanges().isEmpty()) {
            logger.info("Rebalancing is completed.");
            membershipChangeContext = null;
        }

        return membershipChangeContext;
    }

    private void applyMembershipChange(CPGroupMembershipChangeContext ctx, RaftGroup group,
                                       long expectedMembersCommitIndex, long newMembersCommitIndex) {
        CPMember addedMember = ctx.getMemberToAdd();
        CPMember removedMember = ctx.getMemberToRemove();

        if (group.applyMembershipChange(removedMember, addedMember, expectedMembersCommitIndex, newMembersCommitIndex)) {
            logger.fine("Applied add-member: " + (addedMember != null ? addedMember : "-") + " and remove-member: "
                    + (removedMember != null ? removedMember : "-") + " in "  + group.id()
                    + " with new members commit index: " + newMembersCommitIndex);
            if (getLocalMember().equals(addedMember)) {
                // we are the added member to the group, we can try to create the local raft node if not created already
                raftService.createRaftNode(group.id(), group.members());
            } else if (addedMember != null) {
                // publish group-info to the joining member
                Operation op = new CreateRaftNodeOp(group.id(), group.initialMembers());
                nodeEngine.getOperationService().send(op, addedMember.getAddress());
            }
        } else {
            logger.severe("Could not apply add-member: " + (addedMember != null ? addedMember : "-")
                    + " and remove-member: " + (removedMember != null ? removedMember : "-") + " in "  + group
                    + " with new members commit index: " + newMembersCommitIndex + " expected members commit index: "
                    + expectedMembersCommitIndex + " known members commit index: " + group.getMembersCommitIndex());
        }
    }

    private boolean checkSafeToRemove(CPMember leavingMember) {
        if (leavingMember == null) {
            return false;
        }

        for (RaftGroup group : groups.values()) {
            if (group.containsMember(leavingMember)) {
                if (group.status() == DESTROYED) {
                    logger.warning("Leaving " + leavingMember + " was in the destroyed " + group.id());
                } else {
                    return false;
                }
            }
        }

        return true;
    }

    private List<CPGroupMembershipChangeContext> getGroupMembershipChangesForNewMember(CPMember newMember) {
        List<CPGroupMembershipChangeContext> changes = new ArrayList<CPGroupMembershipChangeContext>();
        for (RaftGroup group : groups.values()) {
            if (group.status() == ACTIVE && group.initialMemberCount() > group.memberCount()) {
                checkState(!group.memberImpls().contains(newMember), group + " already contains: " + newMember);

                changes.add(new CPGroupMembershipChangeContext(group.id(), group.getMembersCommitIndex(), group.memberImpls(),
                        newMember, null));
            }
        }

        return changes;
    }

    public Collection<CPMember> getActiveMembers() {
        return activeMembers;
    }

    public void setActiveMembers(Collection<CPMember> members) {
        if (!isDiscoveryCompleted()) {
            logger.fine("Ignore received active members " + members + ", discovery is in progress.");
            return;
        }
        checkNotNull(members);
        checkTrue(members.size() > 1, "active members must contain at least 2 members: " + members);
        checkState(getLocalMember() == null, "This node is already part of CP members!");

        logger.fine("Setting active members to " + members);
        doSetActiveMembers(unmodifiableCollection(new LinkedHashSet<CPMember>(members)));
    }

    private void updateInvocationManagerMembers(Collection<CPMember> members) {
        raftService.getInvocationManager().getRaftInvocationContext().setMembers(members);
    }

    public Collection<CPGroupId> getDestroyingGroupIds() {
        Collection<CPGroupId> groupIds = new ArrayList<CPGroupId>();
        for (RaftGroup group : groups.values()) {
            if (group.status() == DESTROYING) {
                groupIds.add(group.id());
            }
        }
        return groupIds;
    }

    public MembershipChangeContext getMembershipChangeContext() {
        return membershipChangeContext;
    }

    boolean isMetadataGroupLeader() {
        CPMember member = getLocalMember();
        if (member == null) {
            return false;
        }
        RaftNode raftNode = raftService.getRaftNode(METADATA_GROUP_ID);
        // even if the local leader information is stale, it is fine.
        return raftNode != null && !raftNode.isTerminatedOrSteppedDown() && member.equals(raftNode.getLeader());
    }

    /**
     * this method is idempotent
     */
    public void addActiveMember(CPMember member) {
        checkNotNull(member);
        checkIfMetadataRaftGroupInitialized();

        if (activeMembers.contains(member)) {
            logger.fine(member + " already exists. Silently returning from addActiveMember().");
            return;
        }

        checkState(membershipChangeContext == null,
                "Cannot rebalance CP groups because there is ongoing " + membershipChangeContext);

        Collection<CPMember> newMembers = new LinkedHashSet<CPMember>(activeMembers);
        newMembers.add(member);
        doSetActiveMembers(unmodifiableCollection(newMembers));
        logger.info("Added " + member + ". Active members are " + newMembers);

        List<CPGroupMembershipChangeContext> changes = getGroupMembershipChangesForNewMember(member);
        if (changes.size() > 0) {
            logger.info("CP group rebalancing is triggered for " + changes);
            membershipChangeContext = new MembershipChangeContext(null, changes);
        }
    }

    private void removeActiveMember(CPMember member) {
        Collection<CPMember> newMembers = new LinkedHashSet<CPMember>(activeMembers);
        newMembers.remove(member);
        doSetActiveMembers(unmodifiableCollection(newMembers));
    }

    private void doSetActiveMembers(Collection<CPMember> members) {
        activeMembers = unmodifiableCollection(members);
        updateInvocationManagerMembers(members);
        raftService.updateMissingMembers();
        broadcastActiveMembers();
    }

    private void checkIfMetadataRaftGroupInitialized() {
        if (!groups.containsKey(METADATA_GROUP_ID)) {
            throw new MetadataRaftGroupNotInitializedException();
        }
    }

    void broadcastActiveMembers() {
        if (getLocalMember() == null) {
            return;
        }
        Collection<CPMember> members = activeMembers;
        if (members.isEmpty()) {
            return;
        }

        Set<Address> addresses = new HashSet<Address>(members.size());
        for (CPMember member : members) {
            addresses.add(member.getAddress());
        }

        Set<Member> clusterMembers = nodeEngine.getClusterService().getMembers();
        OperationService operationService = nodeEngine.getOperationService();
        Operation op = new SendActiveCPMembersOp(getActiveMembers());
        for (Member member : clusterMembers) {
            if (addresses.contains(member.getAddress()) || member.getAddress().equals(nodeEngine.getThisAddress())) {
                continue;
            }
            operationService.send(op, member.getAddress());
        }
    }

    boolean isDiscoveryCompleted() {
        return discoveryCompleted.get();
    }

    public void disableDiscovery() {
        logger.info("Initial discovery is already completed. Disabling discovery...");
        if (discoveryCompleted.compareAndSet(false, true)) {
            localMember.set(null);
        }
    }

    private class BroadcastActiveCPMembersTask implements Runnable {
        @Override
        public void run() {
            if (!isMetadataGroupLeader()) {
                return;
            }
            broadcastActiveMembers();
        }
    }

    private class DiscoverInitialCPMembersTask implements Runnable {

        private Collection<Member> latestMembers = Collections.emptySet();

        @Override
        public void run() {
            if (isDiscoveryCompleted()) {
                return;
            }
            Collection<Member> members = nodeEngine.getClusterService().getMembers();
            for (Member member : latestMembers) {
                if (!members.contains(member)) {
                    logger.severe(member + " is removed while discovering initial CP members!");
                    terminateNode();
                    return;
                }
            }
            latestMembers = members;

            if (members.size() < config.getCPMemberCount()) {
                logger.warning("Waiting for " + config.getCPMemberCount() + " CP members to join the cluster. "
                        + "Current CP members count: " + members.size());
                ExecutionService executionService = nodeEngine.getExecutionService();
                executionService.schedule(this, DISCOVER_INITIAL_CP_MEMBERS_TASK_DELAY_MILLIS, MILLISECONDS);
                return;
            }

            List<CPMember> cpMembers = getInitialCPMembers(members);
            logger.fine("Initial CP members: " + cpMembers);
            if (!cpMembers.contains(getLocalMember())) {
                logger.info("I am not one of initial CP members! I'll serve as an AP member. CP members: " + cpMembers);
                localMember.set(null);
                disableDiscovery();
                return;
            }

            activeMembers = unmodifiableCollection(new LinkedHashSet<CPMember>(cpMembers));
            updateInvocationManagerMembers(activeMembers);

            if (!commitInitialMetadataRaftGroup(cpMembers)) {
                terminateNode();
                return;
            }

            broadcastActiveMembers();
            scheduleRaftGroupMembershipManagementTasks();
            logger.info("CP members: " + activeMembers + ", local: " + getLocalMember());
            discoveryCompleted.set(true);
        }

        @SuppressWarnings("unchecked")
        private boolean commitInitialMetadataRaftGroup(List<CPMember> initialCPMembers) {
            int metadataGroupSize = config.getGroupSize();
            List<CPMember> metadataMembers = initialCPMembers.subList(0, metadataGroupSize);
            try {
                if (metadataMembers.contains(getLocalMember())) {
                    raftService.createRaftNode(METADATA_GROUP_ID, (Collection) metadataMembers);
                }

                RaftOp op = new CreateMetadataRaftGroupOp(initialCPMembers, metadataGroupSize);
                raftService.getInvocationManager().invoke(METADATA_GROUP_ID, op).get();
                logger.info("METADATA CP group is created with " + metadataMembers);
            } catch (Exception e) {
                logger.severe("Could not create METADATA CP group with " + metadataMembers, e);
                return false;
            }
            return true;
        }

        private void terminateNode() {
            ((NodeEngineImpl) nodeEngine).getNode().shutdown(true);
        }

        private List<CPMember> getInitialCPMembers(Collection<Member> members) {
            assert members.size() >= config.getCPMemberCount();
            List<Member> memberList = new ArrayList<Member>(members).subList(0, config.getCPMemberCount());
            List<CPMember> cpMembers = new ArrayList<CPMember>(config.getCPMemberCount());
            for (Member member : memberList) {
                cpMembers.add(new CPMember(member));
            }

            sort(cpMembers, new CPMemberComparator());
            return cpMembers;
        }
    }

    private static class CPMemberComparator implements Comparator<CPMember> {
        @Override
        public int compare(CPMember e1, CPMember e2) {
            return e1.getUuid().compareTo(e2.getUuid());
        }
    }
}
