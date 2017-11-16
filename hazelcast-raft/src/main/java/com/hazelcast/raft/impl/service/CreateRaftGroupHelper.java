package com.hazelcast.raft.impl.service;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.service.proxy.CreateRaftGroupReplicatingOperation;
import com.hazelcast.raft.impl.service.proxy.RaftReplicatingOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.function.Supplier;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.impl.service.RaftInvocationHelper.invokeOnLeader;

/**
 * TODO: Javadoc Pending...
 *
 */
public final class CreateRaftGroupHelper {

    public static ICompletableFuture createRaftGroupAsync(NodeEngine nodeEngine, final String serviceName,
            final String raftName, final int nodeCount) {
        return invokeOnLeader(nodeEngine, new Supplier<RaftReplicatingOperation>() {
            @Override
            public RaftReplicatingOperation get() {
                return new CreateRaftGroupReplicatingOperation(serviceName, raftName, nodeCount);
            }
        }, RaftService.METADATA_RAFT);
    }

    public static void createRaftGroup(NodeEngine nodeEngine, String serviceName, String raftName, int nodeCount)
            throws ExecutionException, InterruptedException {
        Collection<RaftEndpoint> endpoints =
                (Collection<RaftEndpoint>) createRaftGroupAsync(nodeEngine, serviceName, raftName, nodeCount).get();
        RaftService raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
        if (endpoints.contains(raftService.getLocalEndpoint())) {
            ensureRaftGroupOnLocal(nodeEngine, raftName);
        }
    }

    public static void ensureRaftGroupOnLocal(NodeEngine nodeEngine, String raftName) throws InterruptedException {
        RaftService raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
        if (raftService.getLocalEndpoint() == null) {
            return;
        }
        while (raftService.getRaftGroupInfo(raftName) == null) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        }
    }
}
