/*
 *  Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.raft.service.semaphore;

import com.hazelcast.config.Config;
import com.hazelcast.config.raft.RaftGroupConfig;
import com.hazelcast.config.raft.RaftSemaphoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.HazelcastRaftTestSupport;
import com.hazelcast.raft.service.semaphore.proxy.RaftSemaphoreProxy;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.raft.service.spi.RaftProxyFactory.create;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftSemaphoreBasicTest extends HazelcastRaftTestSupport {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private HazelcastInstance[] instances;
    private ISemaphore semaphore;
    private String name = "semaphore";
    private String groupName = "semaphore";
    private int groupSize = 3;

    @Before
    public void setup() {
        instances = createInstances();

        semaphore = createSemaphore(name);
        assertNotNull(semaphore);
    }

    protected HazelcastInstance[] createInstances() {
        return newInstances(groupSize);
    }

    protected ISemaphore createSemaphore(String name) {
        return create(instances[RandomPicker.getInt(instances.length)], RaftSemaphoreService.SERVICE_NAME, name);
    }

    @Test
    public void testInit() {
        assertTrue(semaphore.init(7));
        assertEquals(7, semaphore.availablePermits());
    }

    @Test
    public void testInitFails_whenAlreadyInitialized() {
        assertTrue(semaphore.init(7));
        assertFalse(semaphore.init(7));
        //...
    }

    @Test
    public void testAcquire() throws InterruptedException {
        assertTrue(semaphore.init(7));

        semaphore.acquire();
        assertEquals(6, semaphore.availablePermits());

        semaphore.acquire(3);
        assertEquals(3, semaphore.availablePermits());
    }

    @Test
    public void testRelease() throws InterruptedException {
        assertTrue(semaphore.init(7));
        semaphore.acquire();
        semaphore.release();
        assertEquals(7, semaphore.availablePermits());
    }

    @Test(expected = IllegalStateException.class)
    public void testRelease_whenNotAcquired() {
        assertTrue(semaphore.init(7));
        semaphore.release();
    }

    @Test
    public void testAcquire_afterRelease() throws InterruptedException {
        assertTrue(semaphore.init(1));
        semaphore.acquire();

        spawn(new Runnable() {
            @Override
            public void run() {
                sleepSeconds(5);
                semaphore.release();
            }
        });

        semaphore.acquire();
    }

    protected RaftGroupId getGroupId(ISemaphore semaphore) {
        return ((RaftSemaphoreProxy) semaphore).getGroupId();
    }

    @Override
    protected Config createConfig(int groupSize, int metadataGroupSize) {
        Config config = super.createConfig(groupSize, metadataGroupSize);
        config.getRaftConfig().addGroupConfig(new RaftGroupConfig(groupName, groupSize));

        RaftSemaphoreConfig semaphoreConfig = new RaftSemaphoreConfig(name, groupName);
        config.addRaftSemaphoreConfig(semaphoreConfig);
        return config;
    }
}
