package com.hazelcast.raft.service.countdownlatch;

import com.hazelcast.config.Config;
import com.hazelcast.config.raft.RaftConfig;
import com.hazelcast.config.raft.RaftCountDownLatchConfig;
import com.hazelcast.config.raft.RaftGroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.service.HazelcastRaftTestSupport;
import com.hazelcast.raft.service.countdownlatch.proxy.RaftCountDownLatchProxy;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.impl.RaftUtil.getSnapshotEntry;
import static com.hazelcast.raft.service.spi.RaftProxyFactory.create;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftCountDownLatchAdvancedTest extends HazelcastRaftTestSupport {

    private static final int LOG_ENTRY_COUNT_TO_SNAPSHOT = 10;

    private HazelcastInstance[] instances;
    private ICountDownLatch latch;
    private String name = "latch";
    private int groupSize = 3;

    @Before
    public void setup() {
        instances = createInstances();

        latch = createLatch(name);
        assertNotNull(latch);
    }

    protected HazelcastInstance[] createInstances() {
        return newInstances(groupSize);
    }

    protected ICountDownLatch createLatch(String name) {
        return create(instances[RandomPicker.getInt(instances.length)], RaftCountDownLatchService.SERVICE_NAME, name);
    }

    @Override
    protected Config createConfig(int groupSize, int metadataGroupSize) {
        Config config = super.createConfig(groupSize, metadataGroupSize);
        RaftConfig raftConfig = config.getRaftConfig();
        raftConfig.getRaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(LOG_ENTRY_COUNT_TO_SNAPSHOT);
        raftConfig.addGroupConfig(new RaftGroupConfig(name, groupSize));

        RaftCountDownLatchConfig latchConfig = new RaftCountDownLatchConfig(name, name);
        config.addRaftCountDownLatchConfig(latchConfig);
        return config;
    }

    @Test
    public void testSuccessfulAwaitClearsWaitTimeouts() {
        latch.trySetCount(1);

        RaftGroupId groupId = getGroupId(latch);
        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        RaftCountDownLatchService service = getNodeEngineImpl(leader).getService(RaftCountDownLatchService.SERVICE_NAME);
        final CountDownLatchRegistry registry = service.getRegistryOrNull(groupId);

        final CountDownLatch threadLatch = new CountDownLatch(1);
        spawn(new Runnable() {
            @Override
            public void run() {
                try {
                    latch.await(10, MINUTES);
                    threadLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFalse(registry.getWaitTimeouts().isEmpty());
            }
        });

        latch.countDown();

        assertOpenEventually(threadLatch);

        assertTrue(registry.getWaitTimeouts().isEmpty());
    }

    @Test
    public void testFailedAwaitClearsWaitTimeouts() throws InterruptedException {
        latch.trySetCount(1);

        RaftGroupId groupId = getGroupId(latch);
        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        RaftCountDownLatchService service = getNodeEngineImpl(leader).getService(RaftCountDownLatchService.SERVICE_NAME);
        final CountDownLatchRegistry registry = service.getRegistryOrNull(groupId);

        boolean success = latch.await(1, TimeUnit.SECONDS);

        assertFalse(success);
        assertTrue(registry.getWaitTimeouts().isEmpty());
    }

    @Test
    public void testDestroyClearsWaitTimeouts() {
        latch.trySetCount(1);

        RaftGroupId groupId = getGroupId(latch);
        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        RaftCountDownLatchService service = getNodeEngineImpl(leader).getService(RaftCountDownLatchService.SERVICE_NAME);
        final CountDownLatchRegistry registry = service.getRegistryOrNull(groupId);

        spawn(new Runnable() {
            @Override
            public void run() {
                try {
                    latch.await(10, MINUTES);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFalse(registry.getWaitTimeouts().isEmpty());
            }
        });

        latch.destroy();

        assertTrue(registry.getWaitTimeouts().isEmpty());
    }

    @Test
    public void testNewRaftGroupMemberSchedulesTimeoutsWithSnapshot() throws ExecutionException, InterruptedException {
        for (int i = 0; i < LOG_ENTRY_COUNT_TO_SNAPSHOT; i++) {
            latch.trySetCount(1);
            latch.countDown();
            latch.await(1, TimeUnit.SECONDS);
        }

        latch.trySetCount(1);

        spawn(new Runnable() {
            @Override
            public void run() {
                try {
                    latch.await(10, MINUTES);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        final RaftGroupId groupId = getGroupId(latch);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftNodeImpl raftNode = getRaftNode(instance, groupId);
                    assertNotNull(raftNode);
                    assertTrue(getSnapshotEntry(raftNode).index() > 0);
                }
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                HazelcastInstance leader = getLeaderInstance(instances, groupId);
                RaftCountDownLatchService service = getNodeEngineImpl(leader).getService(RaftCountDownLatchService.SERVICE_NAME);
                CountDownLatchRegistry registry = service.getRegistryOrNull(groupId);
                assertFalse(registry.getWaitTimeouts().isEmpty());
            }
        });

        instances[1].shutdown();

        final HazelcastInstance newInstance = factory.newHazelcastInstance(createConfig(groupSize, groupSize));
        getRaftService(newInstance).triggerRaftMemberPromotion().get();
        getRaftService(newInstance).triggerRebalanceRaftGroups().get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftCountDownLatchService service = getNodeEngineImpl(newInstance).getService(RaftCountDownLatchService.SERVICE_NAME);
                CountDownLatchRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertFalse(registry.getWaitTimeouts().isEmpty());
                Assert.assertEquals(1, registry.getRemainingCount(name));
            }
        });
    }

    private RaftGroupId getGroupId(ICountDownLatch latch) {
        return ((RaftCountDownLatchProxy) latch).getGroupId();
    }
}
