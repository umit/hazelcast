package com.hazelcast.raft.impl.service;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.MembershipChangeType;
import com.hazelcast.raft.exception.MemberAlreadyExistsException;
import com.hazelcast.raft.exception.MemberDoesNotExistException;
import com.hazelcast.raft.exception.RaftGroupTerminatedException;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.service.operation.metadata.CompleteDestroyRaftGroupsOperation;
import com.hazelcast.raft.impl.service.operation.metadata.CompleteShutdownEndpointOperation;
import com.hazelcast.raft.impl.service.proxy.DefaultRaftGroupReplicateOperation;
import com.hazelcast.raft.impl.service.proxy.MembershipChangeReplicateOperation;
import com.hazelcast.raft.impl.service.proxy.RaftReplicateOperation;
import com.hazelcast.raft.operation.RaftOperation;
import com.hazelcast.raft.operation.TerminateRaftGroupOp;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.function.Supplier;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.impl.service.RaftService.METADATA_GROUP_ID;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftCleanupHandler {

    static final long CLEANUP_TASK_PERIOD = 1000;

    private final NodeEngine nodeEngine;
    private final RaftService raftService;
    private final ILogger logger;

    public RaftCleanupHandler(NodeEngine nodeEngine, RaftService raftService) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.raftService = raftService;
    }

    public void init() {
        if (getLocalEndpoint() == null) {
            return;
        }

        ExecutionService executionService = nodeEngine.getExecutionService();
        // scheduleWithRepetition skips subsequent execution if one is already running.
        executionService.scheduleWithRepetition(new GroupDestroyHandlerTask(),
                CLEANUP_TASK_PERIOD, CLEANUP_TASK_PERIOD, TimeUnit.MILLISECONDS);
        executionService.scheduleWithRepetition(new MemberShutdownHandlerTask(),
                CLEANUP_TASK_PERIOD, CLEANUP_TASK_PERIOD, TimeUnit.MILLISECONDS);
    }

    private RaftEndpoint getLocalEndpoint() {
        return raftService.getMetadataManager().getLocalEndpoint();
    }

    private boolean shouldRunCleanupTask() {
        RaftNode raftNode = raftService.getRaftNode(RaftService.METADATA_GROUP_ID);
        // even if the local leader information is stale, it is fine.
        return getLocalEndpoint().equals(raftNode.getLeader());
    }

    private class GroupDestroyHandlerTask implements Runnable {

        @Override
        public void run() {
            if (!shouldRunCleanupTask()) {
                return;
            }

            // we are reading the destroying group ids locally, since we know they are committed.
            Collection<RaftGroupId> destroyingRaftGroupIds = raftService.getMetadataManager().getDestroyingRaftGroupIds();
            if (destroyingRaftGroupIds.isEmpty()) {
                return;
            }

            Map<RaftGroupId, Future<Object>> futures = new HashMap<RaftGroupId, Future<Object>>();
            for (final RaftGroupId groupId : destroyingRaftGroupIds) {
                Future<Object> future = invoke(new Supplier<RaftReplicateOperation>() {
                    @Override
                    public RaftReplicateOperation get() {
                        return new DefaultRaftGroupReplicateOperation(groupId, new TerminateRaftGroupOp());
                    }
                });
                futures.put(groupId, future);
            }

            final Set<RaftGroupId> terminatedGroupIds = new HashSet<RaftGroupId>();
            for (Map.Entry<RaftGroupId, Future<Object>> e : futures.entrySet()) {
                if (isTerminated(e.getKey(), e.getValue())) {
                    terminatedGroupIds.add(e.getKey());
                }
            }

            if (terminatedGroupIds.isEmpty()) {
                return;
            }

            commitDestroyedRaftGroups(terminatedGroupIds);
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
            Future<Collection<RaftGroupId>> f = invoke(new Supplier<RaftReplicateOperation>() {
                @Override
                public RaftReplicateOperation get() {
                    RaftOperation raftOperation = new CompleteDestroyRaftGroupsOperation(destroyedGroupIds);
                    return new DefaultRaftGroupReplicateOperation(METADATA_GROUP_ID, raftOperation);
                }
            });

            try {
                f.get();
                logger.info("Terminated raft groups: " + destroyedGroupIds + " are committed.");
            } catch (Exception e) {
                logger.severe("Cannot commit terminated raft groups: " + destroyedGroupIds, e);
            }
        }
    }

    private class MemberShutdownHandlerTask implements Runnable {

        @Override
        public void run() {
            if (!shouldRunCleanupTask()) {
                return;
            }

            // we are reading the shutting down endpoints locally, since we know they are committed.
            RaftEndpoint shuttingDownEndpoint = raftService.getMetadataManager().getShuttingDownEndpoint();
            if (shuttingDownEndpoint == null) {
                return;
            }
            handleEndpointShutdown(shuttingDownEndpoint);
        }

        private void handleEndpointShutdown(final RaftEndpoint endpoint) {
            logger.severe("HANDLING SHUTDOWN FOR " + endpoint);
            RaftMetadataManager metadataManager = raftService.getMetadataManager();
            Map<RaftGroupId, RaftEndpoint> substitutes = metadataManager.getSubstitutesFor(endpoint);
            logger.severe("SUBSTITUTES: " + substitutes);

            Map<RaftGroupId, Future<Object>> futures = new HashMap<RaftGroupId, Future<Object>>();

            for (Map.Entry<RaftGroupId, RaftEndpoint> entry : substitutes.entrySet()) {
                final RaftGroupId groupId = entry.getKey();
                final RaftEndpoint substitute = entry.getValue();
                logger.severe("WILL SUBSTITUTE " + endpoint + " with " + substitute + " in " + groupId);
                if (substitute == null) {
                    // no substitute is found
                    continue;
                }
                ICompletableFuture<Object> future = invoke(new Supplier<RaftReplicateOperation>() {
                    @Override
                    public RaftReplicateOperation get() {
                        return new MembershipChangeReplicateOperation(groupId, substitute, MembershipChangeType.ADD);
                    }
                });
                futures.put(groupId, future);
            }

            for (Map.Entry<RaftGroupId, Future<Object>> entry : futures.entrySet()) {
                if (!isMemberAdded(entry.getKey(), entry.getValue())) {
                    return;
                }
            }

            futures.clear();

            for (Map.Entry<RaftGroupId, RaftEndpoint> entry : substitutes.entrySet()) {
                final RaftGroupId groupId = entry.getKey();
                ICompletableFuture<Object> future = invoke(new Supplier<RaftReplicateOperation>() {
                    @Override
                    public RaftReplicateOperation get() {
                        return new MembershipChangeReplicateOperation(groupId, endpoint, MembershipChangeType.REMOVE);
                    }
                });
                futures.put(groupId, future);
            }

            for (Map.Entry<RaftGroupId, Future<Object>> entry : futures.entrySet()) {
                if (!isMemberRemoved(entry.getKey(), entry.getValue())) {
                    return;
                }
            }

            completeShutdownOnMetadata(endpoint);
            removeFromMetadataGroup(endpoint);
        }

        private void completeShutdownOnMetadata(final RaftEndpoint endpoint) {
            ICompletableFuture<Object> future = invoke(new Supplier<RaftReplicateOperation>() {
                @Override
                public RaftReplicateOperation get() {
                    return new DefaultRaftGroupReplicateOperation(METADATA_GROUP_ID,
                            new CompleteShutdownEndpointOperation(endpoint));
                }
            });

            try {
                future.get();
                logger.info(endpoint + " is safe to shutdown");
            } catch (Exception e) {
                logger.severe("Cannot commit shutdown completion for: " + endpoint, e);
            }
        }

        private void removeFromMetadataGroup(final RaftEndpoint endpoint) {
            ICompletableFuture<Object> future = invoke(new Supplier<RaftReplicateOperation>() {
                @Override
                public RaftReplicateOperation get() {
                    return new MembershipChangeReplicateOperation(METADATA_GROUP_ID, endpoint, MembershipChangeType.REMOVE);
                }
            });

            try {
                future.get();
                logger.info(endpoint + " is removed from metadata cluster.");
            } catch (Exception e) {
                logger.severe("Cannot commit remove for: " + endpoint, e);
            }
        }

        private boolean isMemberAdded(RaftGroupId groupId, Future<Object> future) {
            try {
                future.get();
                return true;
            }  catch (InterruptedException e) {
                logger.severe("Cannot get result of MEMBER ADD commit to " + groupId, e);
                return false;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof MemberAlreadyExistsException) {
                    return true;
                }
                logger.severe("Cannot get result of MEMBER ADD commit to " + groupId, e);
                return false;
            }
        }

        private boolean isMemberRemoved(RaftGroupId groupId, Future<Object> future) {
            try {
                future.get();
                return true;
            }  catch (InterruptedException e) {
                logger.severe("Cannot get result of MEMBER REMOVE commit to " + groupId, e);
                return false;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof MemberDoesNotExistException) {
                    return true;
                }
                logger.severe("Cannot get result of MEMBER REMOVE commit to " + groupId, e);
                return false;
            }
        }
    }

    private <T> ICompletableFuture<T> invoke(Supplier<RaftReplicateOperation> supplier) {
        RaftInvocationManager invocationManager = raftService.getInvocationManager();
        return invocationManager.invoke(supplier);
    }
}
