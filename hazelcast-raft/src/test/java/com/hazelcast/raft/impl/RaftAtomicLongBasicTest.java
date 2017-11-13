package com.hazelcast.raft.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLongService;
import com.hazelcast.raft.service.atomiclong.proxy.RaftAtomicLongProxy;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static com.hazelcast.raft.impl.RaftUtil.getLeaderEndpoint;
import static com.hazelcast.raft.impl.RaftUtil.getRaftService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class RaftAtomicLongBasicTest extends HazelcastRaftTestSupport {

    private IAtomicLong atomicLong;
    private final int raftGroupSize = 3;

    @Before
    public void setup() throws ExecutionException, InterruptedException {
        raftAddresses = createRaftAddresses(5);
        instances = newInstances(raftAddresses);

        RaftAtomicLongService service = getNodeEngineImpl(instances[0]).getService(RaftAtomicLongService.SERVICE_NAME);
        String name = "id";

        atomicLong = service.createNew(name, raftGroupSize);
        assertNotNull(atomicLong);
    }

    @Test
    public void createNewAtomicLong() throws Exception {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int count = 0;
                RaftAtomicLongProxy proxy = (RaftAtomicLongProxy) atomicLong;
                for (HazelcastInstance instance : instances) {
                    RaftNode raftNode = getRaftService(instance).getRaftNode(proxy.getRaftName());
                    if (raftNode != null) {
                        count++;
                        assertNotNull(getLeaderEndpoint(raftNode));
                    }
                }
                assertEquals(raftGroupSize, count);
            }
        });
    }

    @Test
    public void incrementAndGet() {
        assertEquals(0, atomicLong.get());
        assertEquals(1, atomicLong.incrementAndGet());
        assertEquals(1, atomicLong.get());
    }

    @Override
    protected Config createConfig(Address[] addresses) {
        ServiceConfig atomicLongServiceConfig = new ServiceConfig().setEnabled(true)
                .setName(RaftAtomicLongService.SERVICE_NAME).setClassName(RaftAtomicLongService.class.getName());

        Config config = super.createConfig(addresses);
        config.getServicesConfig().addServiceConfig(atomicLongServiceConfig);
        return config;
    }
}
