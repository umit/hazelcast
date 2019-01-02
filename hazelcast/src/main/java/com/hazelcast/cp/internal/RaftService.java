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
import com.hazelcast.core.EndpointIdentifier;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPSubsystemManagementService;
import com.hazelcast.cp.internal.exception.CannotRemoveCPMemberException;
import com.hazelcast.cp.internal.raft.SnapshotAwareService;
import com.hazelcast.cp.internal.raft.impl.RaftIntegration;
import com.hazelcast.cp.internal.raft.impl.RaftNode;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.cp.internal.raft.impl.dto.AppendRequest;
import com.hazelcast.cp.internal.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshot;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteResponse;
import com.hazelcast.cp.internal.raft.impl.dto.VoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.VoteResponse;
import com.hazelcast.cp.internal.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.cp.internal.raftop.metadata.AddCPMemberOp;
import com.hazelcast.cp.internal.raftop.metadata.ForceDestroyRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.GetActiveCPMembersOp;
import com.hazelcast.cp.internal.raftop.metadata.GetActiveRaftGroupIdOp;
import com.hazelcast.cp.internal.raftop.metadata.GetInitialRaftGroupMembersIfCurrentGroupMemberOp;
import com.hazelcast.cp.internal.raftop.metadata.GetRaftGroupIdsOp;
import com.hazelcast.cp.internal.raftop.metadata.GetRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.RaftServicePreJoinOp;
import com.hazelcast.cp.internal.raftop.metadata.TriggerRemoveCPMemberOp;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.GracefulShutdownAwareService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MemberAttributeServiceEvent;
import com.hazelcast.spi.MembershipAwareService;
import com.hazelcast.spi.MembershipServiceEvent;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PreJoinAwareService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.executor.ManagedExecutorService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cp.CPGroup.DEFAULT_GROUP_NAME;
import static com.hazelcast.cp.internal.MetadataRaftGroupManager.METADATA_GROUP_ID;
import static com.hazelcast.cp.internal.RaftGroupMembershipManager.MANAGEMENT_TASK_PERIOD_IN_MILLIS;
import static com.hazelcast.cp.internal.raft.QueryPolicy.LEADER_LOCAL;
import static com.hazelcast.internal.config.ConfigValidator.checkCPSubsystemConfig;
import static com.hazelcast.spi.ExecutionService.ASYNC_EXECUTOR;
import static com.hazelcast.spi.ExecutionService.SYSTEM_EXECUTOR;
import static com.hazelcast.util.Preconditions.checkState;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Contains {@link RaftNode} instances that run the Raft consensus algorithm
 * for the created CP groups. Also implements CP subsystem management methods.
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity", "checkstyle:classdataabstractioncoupling"})
public class RaftService implements ManagedService, SnapshotAwareService<MetadataRaftGroupSnapshot>, GracefulShutdownAwareService,
                                    MembershipAwareService, CPSubsystemManagementService, PreJoinAwareService {

    public static final String SERVICE_NAME = "hz:core:raft";

    private static final long REMOVE_MISSING_MEMBER_TASK_PERIOD_SECONDS = 1;

    private final ConcurrentMap<CPGroupId, RaftNode> nodes = new ConcurrentHashMap<CPGroupId, RaftNode>();
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;

    private final Set<CPGroupId> destroyedGroupIds = newSetFromMap(new ConcurrentHashMap<CPGroupId, Boolean>());

    private final CPSubsystemConfig config;
    private final RaftInvocationManager invocationManager;
    private final MetadataRaftGroupManager metadataGroupManager;

    private final ConcurrentMap<CPMember, Long> missingMembers = new ConcurrentHashMap<CPMember, Long>();

    public RaftService(NodeEngine nodeEngine) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        CPSubsystemConfig cpSubsystemConfig = nodeEngine.getConfig().getCPSubsystemConfig();
        this.config = cpSubsystemConfig != null ? new CPSubsystemConfig(cpSubsystemConfig) : new CPSubsystemConfig();
        checkCPSubsystemConfig(this.config);
        this.metadataGroupManager = new MetadataRaftGroupManager(nodeEngine, this, config);
        this.invocationManager = new RaftInvocationManager(nodeEngine, this);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        if (!metadataGroupManager.initLocalCPMemberOnStartup()) {
            return;
        }

        if (config.getMissingCPMemberAutoRemovalSeconds() > 0) {
            nodeEngine.getExecutionService().scheduleWithRepetition(new AutoRemoveMissingCPMemberTask(),
                    REMOVE_MISSING_MEMBER_TASK_PERIOD_SECONDS, REMOVE_MISSING_MEMBER_TASK_PERIOD_SECONDS, SECONDS);
        }
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
    }

    @Override
    public MetadataRaftGroupSnapshot takeSnapshot(CPGroupId groupId, long commitIndex) {
        return metadataGroupManager.takeSnapshot(groupId, commitIndex);
    }

    @Override
    public void restoreSnapshot(CPGroupId groupId, long commitIndex, MetadataRaftGroupSnapshot snapshot) {
        metadataGroupManager.restoreSnapshot(groupId, commitIndex, snapshot);
    }

    @Override
    public Collection<CPGroupId> getCPGroupIds() {
        return invocationManager.<Collection<CPGroupId>>invoke(METADATA_GROUP_ID, new GetRaftGroupIdsOp()).join();
    }

    @Override
    public RaftGroup getCPGroup(CPGroupId groupId) {
        return invocationManager.<RaftGroup>invoke(METADATA_GROUP_ID, new GetRaftGroupOp(groupId)).join();
    }

    /**
     * this method is NOT idempotent and multiple invocations on the same member can break the whole system !!!
     */
    @Override
    public void resetAndInit() {
        // we should clear the current raft state before resetting the metadata manager
        for (RaftNode node : nodes.values()) {
            node.forceSetTerminatedStatus();
        }
        nodes.clear();
        destroyedGroupIds.clear();

        invocationManager.reset();
        metadataGroupManager.reset();
    }

    @Override
    public ICompletableFuture<Void> promoteToCPMember() {
        checkState(metadataGroupManager.getLocalMember() == null, "We are already a CP member!");

        CPMember member = new CPMember(nodeEngine.getLocalMember());
        logger.info("Adding new CP member: " + member);
        ICompletableFuture<Void> future = invocationManager.invoke(METADATA_GROUP_ID, new AddCPMemberOp(member));

        future.andThen(new ExecutionCallback<Void>() {
            @Override
            public void onResponse(Void response) {
                metadataGroupManager.initPromotedCPMember();
            }

            @Override
            public void onFailure(Throwable t) {
            }
        });
        return future;
    }

    /**
     * this method is idempotent
     */
    @Override
    public ICompletableFuture<Void> removeCPMember(EndpointIdentifier m) {
        CPMember member = (CPMember) m;
        ClusterService clusterService = nodeEngine.getClusterService();

        checkState(clusterService.isMaster(), "Only master can remove a CP member!");
        checkState(clusterService.getMember(member.getAddress()) == null,
                "Cannot remove " + member + ", it is a live member!");

        ManagedExecutorService executor = nodeEngine.getExecutionService().getExecutor(SYSTEM_EXECUTOR);
        final SimpleCompletableFuture<Void> future = new SimpleCompletableFuture<Void>(executor, logger);

        invokeTriggerRemoveMember(member).andThen(new ExecutionCallback<Void>() {
            @Override
            public void onResponse(Void response) {
                future.setResult(response);
            }

            @Override
            public void onFailure(Throwable t) {
                if (t instanceof CannotRemoveCPMemberException) {
                    t = new IllegalStateException(t.getMessage());
                }

                future.setResult(new ExecutionException(t));
            }
        });

        return future;
    }

    /**
     * this method is idempotent
     */
    @Override
    public ICompletableFuture<Void> forceDestroyCPGroup(CPGroupId groupId) {
        return invocationManager.invoke(METADATA_GROUP_ID, new ForceDestroyRaftGroupOp(groupId));
    }

    @Override
    public boolean onShutdown(long timeout, TimeUnit unit) {
        CPMember localMember = getLocalMember();
        if (localMember == null) {
            return true;
        }

        if (metadataGroupManager.getActiveMembers().size() == 1) {
            logger.warning("I am the last...");
            return true;
        }

        logger.fine("Triggering remove member procedure for " + localMember);

        long remainingTimeNanos = unit.toNanos(timeout);
        long start = System.nanoTime();

        ensureTriggerShutdown(localMember, remainingTimeNanos);
        remainingTimeNanos -= (System.nanoTime() - start);

        // wait for us being replaced in all raft groups we are participating
        // and removed from all raft groups
        logger.fine("Waiting remove member procedure to be completed for " + localMember
                + ", remaining time: " + TimeUnit.NANOSECONDS.toMillis(remainingTimeNanos) + " ms.");
        while (remainingTimeNanos > 0) {
            if (isRemoved(localMember)) {
                logger.fine("Remove member procedure completed for " + localMember);
                return true;
            }
            try {
                Thread.sleep(MANAGEMENT_TASK_PERIOD_IN_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            remainingTimeNanos -= MANAGEMENT_TASK_PERIOD_IN_MILLIS;
        }
        logger.fine("Remove member procedure NOT completed for " + localMember + " in " + unit.toMillis(timeout) + " ms.");
        return false;
    }

    private void ensureTriggerShutdown(CPMember member, long remainingTimeNanos) {
        while (remainingTimeNanos > 0) {
            long start = System.nanoTime();
            try {
                // mark us as shutting-down in metadata
                Future<Void> future = invokeTriggerRemoveMember(member);
                future.get(remainingTimeNanos, TimeUnit.NANOSECONDS);
                logger.fine(member + " is marked as being removed.");
                return;
            } catch (CannotRemoveCPMemberException e) {
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

    @Override
    public Operation getPreJoinOperation() {
        boolean master = nodeEngine.getClusterService().isMaster();
        return master ? new RaftServicePreJoinOp(metadataGroupManager.isDiscoveryCompleted()) : null;
    }

    @Override
    public void memberAdded(MembershipServiceEvent event) {
        metadataGroupManager.broadcastActiveMembers();
        updateMissingMembers();
    }

    @Override
    public void memberRemoved(MembershipServiceEvent event) {
        updateMissingMembers();
    }

    @Override
    public void memberAttributeChanged(MemberAttributeServiceEvent event) {
        updateMissingMembers();
    }

    void updateMissingMembers() {
        if (config.getMissingCPMemberAutoRemovalSeconds() == 0 || !metadataGroupManager.isDiscoveryCompleted()) {
            return;
        }

        Collection<CPMember> activeMembers = metadataGroupManager.getActiveMembers();
        missingMembers.keySet().retainAll(activeMembers);

        ClusterService clusterService = nodeEngine.getClusterService();
        for (CPMember cpMember : activeMembers) {
            if (clusterService.getMember(cpMember.getAddress()) == null) {
                if (missingMembers.putIfAbsent(cpMember, System.currentTimeMillis()) == null) {
                    logger.warning(cpMember + " is not present in the cluster. It will be auto-removed after "
                            + config.getMissingCPMemberAutoRemovalSeconds() + " seconds.");
                }
            } else if (missingMembers.remove(cpMember) != null) {
                logger.info(cpMember + " is removed from the missing members list as it is in the cluster.");
            }
        }
    }

    Collection<CPMember> getMissingMembers() {
        return Collections.unmodifiableSet(missingMembers.keySet());
    }

    public Collection<CPGroupId> getCPGroupIdsLocally() {
        return metadataGroupManager.getGroupIds();
    }

    public RaftGroup getCPGroupLocally(CPGroupId groupId) {
        return metadataGroupManager.getRaftGroup(groupId);
    }

    public MetadataRaftGroupManager getMetadataGroupManager() {
        return metadataGroupManager;
    }

    public RaftInvocationManager getInvocationManager() {
        return invocationManager;
    }

    public void handlePreVoteRequest(CPGroupId groupId, PreVoteRequest request) {
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            if (logger.isFineEnabled()) {
                logger.warning("RaftNode[" + groupId + "] does not exist to handle: " + request);
            }
            return;
        }
        node.handlePreVoteRequest(request);
    }

    public void handlePreVoteResponse(CPGroupId groupId, PreVoteResponse response) {
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            if (logger.isFineEnabled()) {
                logger.warning("RaftNode[" + groupId + "] does not exist to handle: " + response);
            }
            return;
        }
        node.handlePreVoteResponse(response);
    }

    public void handleVoteRequest(CPGroupId groupId, VoteRequest request) {
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            if (logger.isFineEnabled()) {
                logger.warning("RaftNode[" + groupId + "] does not exist to handle: " + request);
            }
            return;
        }
        node.handleVoteRequest(request);
    }

    public void handleVoteResponse(CPGroupId groupId, VoteResponse response) {
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            if (logger.isFineEnabled()) {
                logger.warning("RaftNode[" + groupId + "] does not exist to handle: " + response);
            }
            return;
        }
        node.handleVoteResponse(response);
    }

    public void handleAppendEntries(CPGroupId groupId, AppendRequest request) {
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            if (logger.isFineEnabled()) {
                logger.warning("RaftNode[" + groupId + "] does not exist to handle: " + request);
            }
            return;
        }
        node.handleAppendRequest(request);
    }

    public void handleAppendResponse(CPGroupId groupId, AppendSuccessResponse response) {
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            if (logger.isFineEnabled()) {
                logger.warning("RaftNode[" + groupId + "] does not exist to handle: " + response);
            }
            return;
        }
        node.handleAppendResponse(response);
    }

    public void handleAppendResponse(CPGroupId groupId, AppendFailureResponse response) {
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            if (logger.isFineEnabled()) {
                logger.warning("RaftNode[" + groupId + "] does not exist to handle: " + response);
            }
            return;
        }
        node.handleAppendResponse(response);
    }

    public void handleSnapshot(CPGroupId groupId, InstallSnapshot request) {
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            if (logger.isFineEnabled()) {
                logger.warning("RaftNode[" + groupId + "] does not exist to handle: " + request);
            }
            return;
        }
        node.handleInstallSnapshot(request);
    }

    public Collection<RaftNode> getAllRaftNodes() {
        return new ArrayList<RaftNode>(nodes.values());
    }

    public RaftNode getRaftNode(CPGroupId groupId) {
        return nodes.get(groupId);
    }

    public RaftNode getOrInitRaftNode(CPGroupId groupId) {
        RaftNode node = nodes.get(groupId);
        if (node == null && !destroyedGroupIds.contains(groupId)) {
            logger.fine("RaftNode[" + groupId + "] does not exist. Asking to the METADATA CP group...");
            nodeEngine.getExecutionService().execute(ASYNC_EXECUTOR, new InitializeRaftNodeTask(groupId));
        }
        return node;
    }

    public boolean isRaftGroupDestroyed(CPGroupId groupId) {
        return destroyedGroupIds.contains(groupId);
    }

    public CPSubsystemConfig getConfig() {
        return config;
    }

    public CPMember getLocalMember() {
        return metadataGroupManager.getLocalMember();
    }

    public void createRaftNode(CPGroupId groupId, Collection<EndpointIdentifier> members) {
        if (nodes.containsKey(groupId)) {
            return;
        }

        if (destroyedGroupIds.contains(groupId)) {
            logger.warning("Not creating RaftNode[" + groupId + "] since the CP group is already destroyed");
            return;
        }

        RaftIntegration integration = new NodeEngineRaftIntegration(nodeEngine, groupId);
        RaftNodeImpl node = new RaftNodeImpl(groupId, getLocalMember(), members, config.getRaftAlgorithmConfig(), integration);

        if (nodes.putIfAbsent(groupId, node) == null) {
            if (destroyedGroupIds.contains(groupId)) {
                node.forceSetTerminatedStatus();
                logger.warning("Not creating RaftNode[" + groupId + "] since the CP group is already destroyed");
                return;
            }

            node.start();
            logger.info("RaftNode[" + groupId + "] is created with " + members);
        }
    }

    public void destroyRaftNode(CPGroupId groupId) {
        destroyedGroupIds.add(groupId);
        RaftNode node = nodes.remove(groupId);
        if (node != null) {
            node.forceSetTerminatedStatus();
            if (logger.isFineEnabled()) {
                logger.fine("Local RaftNode[" + groupId + "] is destroyed.");
            }
        }
    }

    public CPGroupId createRaftGroupForProxy(String name) {
        String groupName = getGroupNameForProxy(name);

        try {
            CPGroupId groupId = getGroupIdForProxy(name);
            if (groupId != null) {
                return groupId;
            }

            return invocationManager.createRaftGroup(groupName).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Could not create CP group: " + groupName);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Could not create CP group: " + groupName);
        }
    }

    public CPGroupId getGroupIdForProxy(String name) {
        String groupName = getGroupNameForProxy(name);
        return invocationManager.<CPGroupId>invoke(METADATA_GROUP_ID, new GetActiveRaftGroupIdOp(groupName)).join();
    }

    private ICompletableFuture<Void> invokeTriggerRemoveMember(CPMember member) {
        return invocationManager.invoke(METADATA_GROUP_ID, new TriggerRemoveCPMemberOp(member));
    }

    private boolean isRemoved(CPMember member) {
        RaftOp op = new GetActiveCPMembersOp();
        InternalCompletableFuture<List<CPMember>> f = invocationManager.query(METADATA_GROUP_ID, op, LEADER_LOCAL);
        List<CPMember> members = f.join();
        return !members.contains(member);
    }

    public static String withoutDefaultGroupName(String name) {
        name = name.trim();
        int i = name.indexOf("@");
        if (i == -1) {
            return name;
        }

        checkTrue(name.indexOf("@", i + 1) == -1, "Custom group name must be specified at most once");
        String groupName = name.substring(i + 1);
        if (groupName.equals(DEFAULT_GROUP_NAME)) {
            return name.substring(0, i);
        }

        return name;
    }

    public static String getGroupNameForProxy(String name) {
        int i = name.indexOf("@");
        if (i == -1) {
            return DEFAULT_GROUP_NAME;
        }

        checkTrue(i < (name.length() - 1), "Custom CP group name cannot be empty string");
        checkTrue(name.indexOf("@", i + 1) == -1, "Custom group name must be specified at most once");
        String groupName = name.substring(i + 1).trim();
        checkTrue(groupName.length() > 0, "Custom CP group name cannot be empty string");
        return groupName;
    }

    public static String getObjectNameForProxy(String name) {
        int i = name.indexOf("@");
        if (i == -1) {
            return name;
        }

        checkTrue(i < (name.length() - 1), "Object name cannot be empty string");
        checkTrue(name.indexOf("@", i + 1) == -1,
                "Custom CP group name must be specified at most once");
        String objectName = name.substring(0, i).trim();
        checkTrue(objectName.length() > 0, "Object name cannot be empty string");
        return objectName;
    }

    private class InitializeRaftNodeTask implements Runnable {
        private final CPGroupId groupId;

        InitializeRaftNodeTask(CPGroupId groupId) {
            this.groupId = groupId;
        }

        @Override
        public void run() {
            queryInitialMembersFromMetadataRaftGroup();
        }

        private void queryInitialMembersFromMetadataRaftGroup() {
            RaftOp op = new GetRaftGroupOp(groupId);
            ICompletableFuture<RaftGroup> f = invocationManager.query(METADATA_GROUP_ID, op, LEADER_LOCAL);
            f.andThen(new ExecutionCallback<RaftGroup>() {
                @Override
                public void onResponse(RaftGroup group) {
                    if (group != null) {
                        if (group.members().contains(getLocalMember())) {
                            createRaftNode(groupId, group.initialMembers());
                        } else {
                            // I can be the member that is just added to the raft group...
                            queryInitialMembersFromTargetRaftGroup();
                        }
                    } else if (logger.isFineEnabled()) {
                        logger.fine("Cannot get initial members of " + groupId + " from the METADATA CP group");
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    if (logger.isFineEnabled()) {
                        logger.fine("Cannot get initial members of " + groupId + " from the METADATA CP group", t);
                    }
                }
            });
        }

        void queryInitialMembersFromTargetRaftGroup() {
            CPMember localMember = getLocalMember();
            if (localMember == null) {
                return;
            }

            RaftOp op = new GetInitialRaftGroupMembersIfCurrentGroupMemberOp(localMember);
            ICompletableFuture<Collection<EndpointIdentifier>> f = invocationManager.query(groupId, op, LEADER_LOCAL);
            f.andThen(new ExecutionCallback<Collection<EndpointIdentifier>>() {
                @Override
                public void onResponse(Collection<EndpointIdentifier> initialMembers) {
                    createRaftNode(groupId, initialMembers);
                }

                @Override
                public void onFailure(Throwable t) {
                    if (logger.isFineEnabled()) {
                        logger.fine("Cannot get initial members of " + groupId + " from the CP group itself", t);
                    }
                }
            });
        }
    }

    private class AutoRemoveMissingCPMemberTask implements Runnable {
        @Override
        public void run() {
            try {
                if (!nodeEngine.getClusterService().isMaster() || missingMembers.isEmpty()
                        || metadataGroupManager.getMembershipChangeContext() != null) {
                    return;
                }

                for (Entry<CPMember, Long> e : missingMembers.entrySet()) {
                    long missingTimeSeconds = MILLISECONDS.toSeconds(System.currentTimeMillis() - e.getValue());
                    if (missingTimeSeconds >= config.getMissingCPMemberAutoRemovalSeconds()) {
                        CPMember missingMember = e.getKey();
                        logger.warning("Removing " + missingMember + " since it is absent for " + missingTimeSeconds
                                + " seconds.");

                        removeCPMember(missingMember).get();

                        logger.info("Auto-removal of " + missingMember + " is successful.");

                        return;
                    }
            }
            } catch (Exception e) {
                logger.severe("RemoveMissingMembersTask failed", e);
            }
        }
    }

}
