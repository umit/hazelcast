package com.hazelcast.raft.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.impl.service.RaftDataService;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static com.hazelcast.raft.impl.service.CreateRaftGroupHelper.createRaftGroup;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CreateRaftGroupHelperTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;

    @Test
    public void when_raftGroupIsCreatedWithAllCPNodes_then_raftNodeIsCreatedOnAll() throws ExecutionException, InterruptedException {
        int nodeCount = 5;
        Address[] raftAddresses = createAddresses(nodeCount);
        instances = newInstances(raftAddresses);

        createRaftGroup(getNodeEngineImpl(instances[0]), RaftDataService.SERVICE_NAME, "test", nodeCount);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (HazelcastInstance instance : instances) {
                    RaftService service = getNodeEngineImpl(instance).getService(RaftService.SERVICE_NAME);
                    assertNotNull(service.getRaftNode("test"));
                }
            }
        });
    }

    @Test
    public void when_raftGroupIsCreatedWithSomeCPNodes_then_raftNodeIsCreatedOnOnlyTheSelectedEndpoints() throws ExecutionException, InterruptedException {
        final int cpNodeCount = 3;
        int nodeCount = 5;
        Address[] raftAddresses = createAddresses(cpNodeCount);
        instances = newInstances(raftAddresses, nodeCount);

        createRaftGroup(getNodeEngineImpl(instances[0]), RaftDataService.SERVICE_NAME, "test", cpNodeCount);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int count = 0;
                for (HazelcastInstance instance : instances) {
                    RaftService service = getNodeEngineImpl(instance).getService(RaftService.SERVICE_NAME);
                    RaftNode raftNode = service.getRaftNode("test");
                    if (raftNode != null) {
                        count++;
                    }
                }

                assertEquals(cpNodeCount, count);
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

        createRaftGroup(getNodeEngineImpl(instances[0]), RaftDataService.SERVICE_NAME, "test", nodeCount);
        createRaftGroup(getNodeEngineImpl(instances[1]), RaftDataService.SERVICE_NAME, "test", nodeCount);
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
}
