package com.hazelcast.cp.internal;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CPSubsystemImplTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void init() {
        factory = createHazelcastInstanceFactory();
    }

    @Test
    public void test_atomicLong_whenCPSubsystemNotConfigured() {
        HazelcastInstance instance = factory.newHazelcastInstance();
        thrown.expect(HazelcastException.class);

        instance.getCPSubsystem().getAtomicLong("long");
    }

    @Test
    public void test_atomicReference_whenCPSubsystemNotConfigured() {
        HazelcastInstance instance = factory.newHazelcastInstance();
        thrown.expect(HazelcastException.class);

        instance.getCPSubsystem().getAtomicReference("ref");
    }

    @Test
    public void test_lock_whenCPSubsystemNotConfigured() {
        HazelcastInstance instance = factory.newHazelcastInstance();
        thrown.expect(HazelcastException.class);

        instance.getCPSubsystem().getLock("lock");
    }

    @Test
    public void test_semaphore_whenCPSubsystemNotConfigured() {
        HazelcastInstance instance = factory.newHazelcastInstance();
        thrown.expect(HazelcastException.class);

        instance.getCPSubsystem().getSemaphore("semaphore");
    }

    @Test
    public void test_countDownLatch_whenCPSubsystemNotConfigured() {
        HazelcastInstance instance = factory.newHazelcastInstance();
        thrown.expect(HazelcastException.class);

        instance.getCPSubsystem().getAtomicLong("latch");
    }

    @Test
    public void test_cpSubsystemManagementService_whenCPSubsystemNotConfigured() {
        HazelcastInstance instance = factory.newHazelcastInstance();
        thrown.expect(HazelcastException.class);

        instance.getCPSubsystem().getCPSubsystemManagementService();
    }

    @Test
    public void test_cpSessionManagementService_whenCPSubsystemNotConfigured() {
        HazelcastInstance instance = factory.newHazelcastInstance();
        thrown.expect(HazelcastException.class);

        instance.getCPSubsystem().getCPSessionManagementService();
    }

    @Test
    public void test_atomicLong_whenCPSubsystemConfigured() {
        Config config = new Config();
        config.getCPSubsystemConfig().setCPMemberCount(3);

        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        HazelcastInstance instance = factory.newHazelcastInstance(config);

        assertNotNull(instance.getCPSubsystem().getAtomicLong("long"));
    }

    @Test
    public void test_atomicReference_whenCPSubsystemConfigured() {
        Config config = new Config();
        config.getCPSubsystemConfig().setCPMemberCount(3);

        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        HazelcastInstance instance = factory.newHazelcastInstance(config);

        assertNotNull(instance.getCPSubsystem().getAtomicReference("ref"));
    }

    @Test
    public void test_lock_whenCPSubsystemConfigured() {
        Config config = new Config();
        config.getCPSubsystemConfig().setCPMemberCount(3);

        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        HazelcastInstance instance = factory.newHazelcastInstance(config);

        assertNotNull(instance.getCPSubsystem().getLock("lock"));
    }

    @Test
    public void test_semaphore_whenCPSubsystemConfigured() {
        Config config = new Config();
        config.getCPSubsystemConfig().setCPMemberCount(3);

        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        HazelcastInstance instance = factory.newHazelcastInstance(config);

        assertNotNull(instance.getCPSubsystem().getSemaphore("semaphore"));
    }

    @Test
    public void test_countDownLatch_whenCPSubsystemConfigured() {
        Config config = new Config();
        config.getCPSubsystemConfig().setCPMemberCount(3);

        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        HazelcastInstance instance = factory.newHazelcastInstance(config);

        assertNotNull(instance.getCPSubsystem().getCountDownLatch("latch"));
    }

    @Test
    public void test_cpSubsystemManagementService_whenCPSubsystemConfigured() {
        Config config = new Config();
        config.getCPSubsystemConfig().setCPMemberCount(3);

        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        HazelcastInstance instance = factory.newHazelcastInstance(config);

        assertNotNull(instance.getCPSubsystem().getCPSubsystemManagementService());
    }

    @Test
    public void test_cpSessionManagementService_whenCPSubsystemConfigured() {
        Config config = new Config();
        config.getCPSubsystemConfig().setCPMemberCount(3);

        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        HazelcastInstance instance = factory.newHazelcastInstance(config);

        assertNotNull(instance.getCPSubsystem().getCPSessionManagementService());
    }

}
