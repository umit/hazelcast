package com.hazelcast.raft.impl.service;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.exception.RaftGroupTerminatedException;
import com.hazelcast.raft.impl.service.proxy.DefaultRaftGroupReplicateOperation;
import com.hazelcast.raft.impl.service.proxy.RaftReplicateOperation;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.function.Supplier;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftInvocationManagerTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;

    @Test
    public void when_raftGroupIsCreated_then_raftOperationsAreExecuted() throws ExecutionException, InterruptedException {
        int nodeCount = 5;
        Address[] raftAddresses = createAddresses(nodeCount);
        instances = newInstances(raftAddresses);

        RaftInvocationManager invocationService = getRaftInvocationService(instances[0]);
        final RaftGroupId groupId = invocationService.createRaftGroup(RaftDataService.SERVICE_NAME, "test", nodeCount);

        for (int i = 0; i < 100; i++) {
            invocationService.invoke(createRaftAddOperationSupplier(groupId, "val" + i)).get();
        }
    }

    @Test
    public void when_raftGroupIsCreated_then_raftOperationsAreExecutedOnNonCPNode() throws ExecutionException, InterruptedException {
        int cpNodeCount = 5;
        Address[] raftAddresses = createAddresses(cpNodeCount);
        instances = newInstances(raftAddresses, cpNodeCount + 1);

        RaftInvocationManager invocationService = getRaftInvocationService(instances[instances.length - 1]);
        final RaftGroupId groupId = invocationService.createRaftGroup(RaftDataService.SERVICE_NAME, "test", cpNodeCount);

        for (int i = 0; i < 100; i++) {
            invocationService.invoke(createRaftAddOperationSupplier(groupId, "val" + i)).get();
        }
    }

    @Test
    public void when_raftGroupIsDestroyed_then_operationsEventuallyFail() throws ExecutionException, InterruptedException {
        int nodeCount = 3;
        Address[] raftAddresses = createAddresses(nodeCount);
        instances = newInstances(raftAddresses);

        final RaftInvocationManager invocationService = getRaftInvocationService(instances[0]);
        final RaftGroupId groupId = invocationService.createRaftGroup(RaftDataService.SERVICE_NAME, "test", nodeCount);

        invocationService.invoke(createRaftAddOperationSupplier(groupId, "val")).get();

        invocationService.triggerDestroyRaftGroup(groupId);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                try {
                    invocationService.invoke(createRaftAddOperationSupplier(groupId, "val")).get();
                    fail();
                } catch (RaftGroupTerminatedException ignored) {
                }
            }
        });
    }

    private Supplier<RaftReplicateOperation> createRaftAddOperationSupplier(final RaftGroupId groupId, final Object val) {
        return new Supplier<RaftReplicateOperation>() {
            @Override
            public RaftReplicateOperation get() {
                return new DefaultRaftGroupReplicateOperation(groupId, new RaftAddOperation(val));
            }
        };
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
