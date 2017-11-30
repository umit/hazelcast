package com.hazelcast.raft.impl.service;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.service.RaftGroupInfo.RaftGroupStatus;
import com.hazelcast.raft.impl.service.operation.metadata.GetDestroyingRaftGroupsOperation;
import com.hazelcast.raft.impl.service.proxy.DefaultRaftGroupReplicatingOperation;
import com.hazelcast.raft.impl.service.proxy.RaftReplicatingOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.function.Supplier;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.raft.impl.service.util.RaftGroupHelper.createRaftGroup;
import static com.hazelcast.raft.impl.service.util.RaftGroupHelper.triggerDestroyRaftGroupAsync;
import static com.hazelcast.raft.impl.service.util.RaftInvocationHelper.invokeOnLeader;
import static com.hazelcast.raft.impl.service.RaftService.METADATA_GROUP_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftGroupHelperTest
        extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;

    @Test
    public void when_raftGroupIsCreatedWithAllCPNodes_then_raftNodeIsCreatedOnAll() throws ExecutionException, InterruptedException {
        int nodeCount = 5;
        Address[] raftAddresses = createAddresses(nodeCount);
        instances = newInstances(raftAddresses);

        final RaftGroupId groupId =
                createRaftGroup(getNodeEngineImpl(instances[0]), RaftDataService.SERVICE_NAME, "test", nodeCount);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (HazelcastInstance instance : instances) {
                    RaftService service = getNodeEngineImpl(instance).getService(RaftService.SERVICE_NAME);
                    assertNotNull(service.getRaftNode(groupId));
                }
            }
        });
    }

    @Test
    public void when_raftGroupIsCreatedWithSomeCPNodes_then_raftNodeIsCreatedOnOnlyTheSelectedEndpoints() throws ExecutionException, InterruptedException {
        when_raftGroupIsCreatedWithSomeCPNodes_then_raftNodeIsCreatedOnOnlyTheSelectedEndpoints(true);
    }

    @Test
    public void when_raftGroupIsCreatedFromNonCPNode_then_raftNodeIsCreatedOnOnlyTheSelectedEndpoints() throws ExecutionException, InterruptedException {
        when_raftGroupIsCreatedWithSomeCPNodes_then_raftNodeIsCreatedOnOnlyTheSelectedEndpoints(false);
    }

    private void when_raftGroupIsCreatedWithSomeCPNodes_then_raftNodeIsCreatedOnOnlyTheSelectedEndpoints(boolean invokeOnCP)
            throws ExecutionException, InterruptedException {
        int cpNodeCount = 4;
        int nodeCount = 6;
        Address[] raftAddresses = createAddresses(cpNodeCount);
        instances = newInstances(raftAddresses, nodeCount);

        final int newGroupCount = 3;

        HazelcastInstance instance = instances[invokeOnCP ? 0 : instances.length - 1];
        final RaftGroupId groupId =
                createRaftGroup(getNodeEngineImpl(instance), RaftDataService.SERVICE_NAME, "test", newGroupCount);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int count = 0;
                for (HazelcastInstance instance : instances) {
                    RaftService service = getNodeEngineImpl(instance).getService(RaftService.SERVICE_NAME);
                    RaftNode raftNode = service.getRaftNode(groupId);
                    if (raftNode != null) {
                        count++;
                    }
                }

                assertEquals(newGroupCount, count);
            }
        });
    }

    @Test
    public void when_sizeOfRaftGroupIsLargerThanCPNodeCount_then_raftGroupCannotBeCreated() throws ExecutionException, InterruptedException {
        int nodeCount = 3;
        Address[] raftAddresses = createAddresses(nodeCount);
        instances = newInstances(raftAddresses);

        try {
            createRaftGroup(getNodeEngineImpl(instances[0]), RaftDataService.SERVICE_NAME, "test", nodeCount + 1);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void when_raftGroupIsCreatedWithSameSizeMultipleTimes_then_itSucceeds() throws ExecutionException, InterruptedException {
        int nodeCount = 3;
        Address[] raftAddresses = createAddresses(nodeCount);
        instances = newInstances(raftAddresses);

        RaftGroupId groupId1 =
                createRaftGroup(getNodeEngineImpl(instances[0]), RaftDataService.SERVICE_NAME, "test", nodeCount);
        RaftGroupId groupId2 =
                createRaftGroup(getNodeEngineImpl(instances[1]), RaftDataService.SERVICE_NAME, "test", nodeCount);
        assertEquals(groupId1, groupId2);
    }

    @Test
    public void when_raftGroupIsCreatedWithDifferentSizeMultipleTimes_then_itFails() throws ExecutionException, InterruptedException {
        int nodeCount = 3;
        Address[] raftAddresses = createAddresses(nodeCount);
        instances = newInstances(raftAddresses);

        createRaftGroup(getNodeEngineImpl(instances[0]), RaftDataService.SERVICE_NAME, "test", nodeCount);
        try {
            createRaftGroup(getNodeEngineImpl(instances[1]), RaftDataService.SERVICE_NAME, "test", nodeCount - 1);
            fail();
        } catch (IllegalStateException ignored) {
        }
    }

    @Test
    public void when_raftGroupTriggerDestroyIsCommitted_then_raftGroupStatusIsUpdated() throws ExecutionException, InterruptedException {
        int nodeCount = 3;
        Address[] raftAddresses = createAddresses(nodeCount);
        instances = newInstances(raftAddresses);

        NodeEngineImpl nodeEngine = getNodeEngineImpl(instances[0]);
        final RaftGroupId groupId = createRaftGroup(nodeEngine, RaftDataService.SERVICE_NAME, "test", nodeCount);

        invokeOnLeader(nodeEngine, new Supplier<RaftReplicatingOperation>() {
            @Override
            public RaftReplicatingOperation get() {
                return new DefaultRaftGroupReplicatingOperation(groupId, new RaftAddOperation("val"));
            }
        }, groupId).get();

        triggerDestroyRaftGroupAsync(nodeEngine, groupId).get();

        ICompletableFuture<Collection<RaftGroupId>> future = invokeOnLeader(nodeEngine,
                new Supplier<RaftReplicatingOperation>() {
                    @Override
                    public RaftReplicatingOperation get() {
                        return new DefaultRaftGroupReplicatingOperation(METADATA_GROUP_ID, new GetDestroyingRaftGroupsOperation());
                    }
                }, METADATA_GROUP_ID);

        Collection<RaftGroupId> groupIds = future.get();
        assertEquals(1, groupIds.size());
        assertEquals(groupId, groupIds.iterator().next());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (HazelcastInstance instance : instances) {
                    RaftService service = getNodeEngineImpl(instance).getService(RaftService.SERVICE_NAME);
                    RaftGroupInfo groupInfo = service.getRaftGroupInfo(groupId);
                    assertNotNull(groupInfo);
                    assertEquals(RaftGroupStatus.DESTROYED, groupInfo.status());
                    assertNull(service.getRaftNode(groupId));
                }
            }
        });
    }

    @Override
    protected Config createConfig(Address[] raftAddresses) {
        Config config = super.createConfig(raftAddresses);

        ServiceConfig raftTestServiceConfig = new ServiceConfig().setEnabled(true)
                .setName(RaftDataService.SERVICE_NAME)
                .setClassName(RaftDataService.class.getName());
        config.getServicesConfig().addServiceConfig(raftTestServiceConfig);

        return config;
    }
}
