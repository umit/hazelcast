package com.hazelcast.raft.impl.service.util;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.raft.impl.service.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.operation.metadata.CreateRaftGroupReplicatingOperation;
import com.hazelcast.raft.impl.service.operation.metadata.TriggerDestroyRaftGroupOperation;
import com.hazelcast.raft.impl.service.proxy.RaftReplicatingOperation;
import com.hazelcast.raft.impl.service.proxy.DefaultRaftGroupReplicatingOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.function.Supplier;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.impl.service.util.RaftInvocationHelper.invokeOnLeader;
import static com.hazelcast.raft.impl.service.RaftService.METADATA_GROUP_ID;
import static com.hazelcast.raft.impl.service.RaftService.SERVICE_NAME;

/**
 * TODO: Javadoc Pending...
 *
 */
public final class RaftGroupHelper {

    public static ICompletableFuture<RaftGroupId> createRaftGroupAsync(NodeEngine nodeEngine, final String serviceName,
                                                                       final String raftName, final int nodeCount) {
        return invokeOnLeader(nodeEngine, new Supplier<RaftReplicatingOperation>() {
            @Override
            public RaftReplicatingOperation get() {
                return new CreateRaftGroupReplicatingOperation(serviceName, raftName, nodeCount);
            }
        }, METADATA_GROUP_ID);
    }

    public static RaftGroupId createRaftGroup(NodeEngine nodeEngine, String serviceName, String raftName, int nodeCount)
            throws ExecutionException, InterruptedException {
        RaftGroupId raftGroupId = createRaftGroupAsync(nodeEngine, serviceName, raftName, nodeCount).get();
        ensureRaftGroupOnLocal(nodeEngine, raftGroupId);
        return raftGroupId;
    }

    public static void ensureRaftGroupOnLocal(NodeEngine nodeEngine, RaftGroupId groupId) throws InterruptedException {
        RaftService raftService = nodeEngine.getService(SERVICE_NAME);
        if (raftService.getLocalEndpoint() == null) {
            // local member is not in CP group
            return;
        }
        while (raftService.getRaftGroupInfo(groupId) == null) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        }
    }

    public static ICompletableFuture<RaftGroupId> triggerDestroyRaftGroupAsync(NodeEngine nodeEngine, final RaftGroupId groupId) {
        return invokeOnLeader(nodeEngine, new Supplier<RaftReplicatingOperation>() {
            @Override
            public RaftReplicatingOperation get() {
                return new DefaultRaftGroupReplicatingOperation(METADATA_GROUP_ID, new TriggerDestroyRaftGroupOperation(groupId));
            }
        }, METADATA_GROUP_ID);
    }
}
