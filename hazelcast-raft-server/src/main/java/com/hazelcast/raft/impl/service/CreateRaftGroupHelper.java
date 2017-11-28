package com.hazelcast.raft.impl.service;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.raft.impl.service.proxy.CreateRaftGroupReplicatingOperation;
import com.hazelcast.raft.impl.service.proxy.RaftReplicatingOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.function.Supplier;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.impl.service.RaftInvocationHelper.invokeOnLeader;
import static com.hazelcast.raft.impl.service.RaftService.METADATA_GROUP_ID;
import static com.hazelcast.raft.impl.service.RaftService.SERVICE_NAME;

/**
 * TODO: Javadoc Pending...
 *
 */
public final class CreateRaftGroupHelper {

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
        ensureRaftGroupOnLocal(nodeEngine, raftName);
        return raftGroupId;
    }

    public static void ensureRaftGroupOnLocal(NodeEngine nodeEngine, String raftName) throws InterruptedException {
        RaftService raftService = nodeEngine.getService(SERVICE_NAME);
        if (raftService.getLocalEndpoint() == null) {
            // local member is not in CP group
            return;
        }
        while (raftService.getRaftGroupInfo(raftName) == null) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        }
    }
}
