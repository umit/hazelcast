package com.hazelcast.raft.service.lock;

import com.hazelcast.config.Config;
import com.hazelcast.config.raft.RaftConfig;
import com.hazelcast.config.raft.RaftGroupConfig;
import com.hazelcast.config.raft.RaftLockConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.service.HazelcastRaftTestSupport;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.service.lock.proxy.RaftFencedLockProxy;
import com.hazelcast.raft.service.session.SessionManagerService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.RandomPicker;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.impl.RaftUtil.getSnapshotEntry;
import static com.hazelcast.raft.service.lock.RaftFencedLockBasicTest.lockByOtherThread;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftFencedLockAdvancedTest extends HazelcastRaftTestSupport {

    private static final int LOG_ENTRY_COUNT_TO_SNAPSHOT = 10;

    private HazelcastInstance[] instances;
    private FencedLock lock;
    private String name = "lock";
    private int groupSize = 3;

    @Before
    public void setup() {
        instances = createInstances();

        lock = createLock(name);
        assertNotNull(lock);
    }

    private FencedLock createLock(String name) {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(instances[RandomPicker.getInt(instances.length)]);
        RaftService raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
        RaftLockService lockService = nodeEngine.getService(RaftLockService.SERVICE_NAME);

        try {
            RaftGroupId groupId = lockService.createRaftGroup(name).get();
            SessionManagerService sessionManager = nodeEngine.getService(SessionManagerService.SERVICE_NAME);
            return new RaftFencedLockProxy(raftService.getInvocationManager(), sessionManager, groupId, name);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    protected Config createConfig(int groupSize, int metadataGroupSize) {
        Config config = super.createConfig(groupSize, metadataGroupSize);
        RaftConfig raftConfig = config.getRaftConfig();
        raftConfig.getRaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(LOG_ENTRY_COUNT_TO_SNAPSHOT);
        raftConfig.addGroupConfig(new RaftGroupConfig(name, groupSize));

        RaftLockConfig lockConfig = new RaftLockConfig(name, name);
        config.addRaftLockConfig(lockConfig);
        return config;
    }

    protected HazelcastInstance[] createInstances() {
        return newInstances(groupSize);
    }

    @Test
    public void testSuccessfulTryLockClearsWaitTimeouts() {
        lock.lock();

        RaftGroupId groupId = getGroupId(lock);
        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        RaftLockService service = getNodeEngineImpl(leader).getService(RaftLockService.SERVICE_NAME);
        final LockRegistry registry = service.getRegistryOrNull(groupId);

        final CountDownLatch latch = new CountDownLatch(1);
        spawn(new Runnable() {
            @Override
            public void run() {
                try {
                    lock.tryLock(10, MINUTES);
                    latch.countDown();
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

        lock.unlock();

        assertOpenEventually(latch);

        assertTrue(registry.getWaitTimeouts().isEmpty());
    }

    @Test
    public void testFailedTryLockClearsWaitTimeouts() throws InterruptedException {
        lockByOtherThread(lock);

        RaftGroupId groupId = getGroupId(lock);
        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        RaftLockService service = getNodeEngineImpl(leader).getService(RaftLockService.SERVICE_NAME);
        LockRegistry registry = service.getRegistryOrNull(groupId);

        long fence = lock.tryLock(1, TimeUnit.SECONDS);

        assertEquals(RaftLockService.INVALID_FENCE, fence);
        assertTrue(registry.getWaitTimeouts().isEmpty());
    }

    @Test
    public void testDestroyClearsWaitTimeouts() {
        lockByOtherThread(lock);

        RaftGroupId groupId = getGroupId(lock);
        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        RaftLockService service = getNodeEngineImpl(leader).getService(RaftLockService.SERVICE_NAME);
        final LockRegistry registry = service.getRegistryOrNull(groupId);

        spawn(new Runnable() {
            @Override
            public void run() {
                try {
                    lock.tryLock(10, MINUTES);
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

        lock.destroy();

        assertTrue(registry.getWaitTimeouts().isEmpty());
    }

    @Test
    public void testNewRaftGroupMemberSchedulesTimeoutsWithSnapshot() throws ExecutionException, InterruptedException {
        for (int i = 0; i < LOG_ENTRY_COUNT_TO_SNAPSHOT; i++) {
            lock.lock();
            lock.unlock();
        }

        final long fence = this.lock.lock();

        spawn(new Runnable() {
            @Override
            public void run() {
                try {
                    RaftFencedLockAdvancedTest.this.lock.tryLock(10, MINUTES);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        final RaftGroupId groupId = getGroupId(this.lock);

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
                RaftLockService service = getNodeEngineImpl(leader).getService(RaftLockService.SERVICE_NAME);
                LockRegistry registry = service.getRegistryOrNull(groupId);
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
                RaftLockService service = getNodeEngineImpl(newInstance).getService(RaftLockService.SERVICE_NAME);
                LockRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertFalse(registry.getWaitTimeouts().isEmpty());
                assertEquals(fence, registry.getLockFence(name));
            }
        });
    }

    private RaftGroupId getGroupId(FencedLock lock) {
        return ((RaftFencedLockProxy) lock).getGroupId();
    }
}