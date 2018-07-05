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
import com.hazelcast.raft.impl.service.HazelcastRaftTestSupport;
import com.hazelcast.test.AssertTask;
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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.raft.service.spi.RaftProxyFactory.create;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftSessionlessSemaphoreBasicTest extends HazelcastRaftTestSupport {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private HazelcastInstance[] instances;
    private ISemaphore semaphore;
    private String name = "semaphore";
    private String groupName = "semaphore";
    private int groupSize = 3;
    private HazelcastInstance semaphoreInstance;

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
        semaphoreInstance = instances[RandomPicker.getInt(instances.length)];
        return create(semaphoreInstance, RaftSemaphoreService.SERVICE_NAME, name);
    }

    @Test
    public void testInit() {
        assertTrue(semaphore.init(7));
        assertEquals(7, semaphore.availablePermits());
    }

    @Test
    public void testInitFails_whenAlreadyInitialized() {
        assertTrue(semaphore.init(7));
        assertFalse(semaphore.init(5));
        assertEquals(7, semaphore.availablePermits());
    }

    @Test
    public void testAcquire() throws InterruptedException {
        int numberOfPermits = 20;
        assertTrue(semaphore.init(numberOfPermits));
        for (int i = 0; i < numberOfPermits; i++) {
            assertEquals(numberOfPermits - i, semaphore.availablePermits());
            semaphore.acquire();
        }

        assertEquals(semaphore.availablePermits(), 0);
    }

    @Test
    public void testAcquire_whenNoPermits() {
        semaphore.init(0);
        final Future future = spawn(new Runnable() {
            @Override
            public void run() {
                try {
                    semaphore.acquire();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertFalse(future.isDone());
                assertEquals(0, semaphore.availablePermits());
            }
        }, 5);
    }

    @Test
    public void testAcquire_whenNoPermits_andSemaphoreDestroyed() throws Exception {
        semaphore.init(0);
        Future future = spawn(new Runnable() {
            @Override
            public void run() {
                try {
                    semaphore.acquire();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        semaphore.destroy();
        try {
            future.get();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testRelease() {
        int numberOfPermits = 20;
        for (int i = 0; i < numberOfPermits; i++) {
            assertEquals(i, semaphore.availablePermits());
            semaphore.release();
        }

        assertEquals(semaphore.availablePermits(), numberOfPermits);
    }

    @Test
    public void testAllowNegativePermits() {
        assertTrue(semaphore.init(10));

        semaphore.reducePermits(15);

        assertEquals(-5, semaphore.availablePermits());

        semaphore.release(10);

        assertEquals(5, semaphore.availablePermits());
    }

    @Test
    public void testNegativePermitsJucCompatibility() {
        assertTrue(semaphore.init(0));

        semaphore.reducePermits(100);
        semaphore.release(10);

        assertEquals(-90, semaphore.availablePermits());
        assertEquals(-90, semaphore.drainPermits());

        semaphore.release(10);

        assertEquals(10, semaphore.availablePermits());
        assertEquals(10, semaphore.drainPermits());
    }


    @Test
    public void testIncreasePermits() {
        assertTrue(semaphore.init(10));

        assertEquals(10, semaphore.availablePermits());

        semaphore.increasePermits(100);

        assertEquals(110, semaphore.availablePermits());
    }

    @Test
    public void testRelease_whenArgumentNegative() {
        try {
            semaphore.release(-5);
            fail();
        } catch (IllegalArgumentException expected) {
        }
        assertEquals(0, semaphore.availablePermits());
    }

    @Test
    public void testRelease_whenBlockedAcquireThread() throws InterruptedException {
        semaphore.init(0);

        new Thread() {
            @Override
            public void run() {
                try {
                    semaphore.acquire();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        semaphore.release();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(0, semaphore.availablePermits());
            }
        });
    }

    @Test
    public void testMultipleAcquire() throws InterruptedException {
        int numberOfPermits = 20;

        assertTrue(semaphore.init(numberOfPermits));
        for (int i = 0; i < numberOfPermits; i += 5) {
            assertEquals(numberOfPermits - i, semaphore.availablePermits());
            semaphore.acquire(5);
        }
        assertEquals(semaphore.availablePermits(), 0);
    }

    @Test
    public void testMultipleAcquire_whenNegative() throws InterruptedException {
        int numberOfPermits = 10;
        semaphore.init(numberOfPermits);
        try {
            semaphore.acquire(-5);
            fail();
        } catch (IllegalArgumentException expected) {
        }
        assertEquals(10, semaphore.availablePermits());

    }

    @Test
    public void testMultipleAcquire_whenNotEnoughPermits() throws InterruptedException {
        int numberOfPermits = 5;
        semaphore.init(numberOfPermits);

        final Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    semaphore.acquire(6);
                    assertEquals(5, semaphore.availablePermits());
                    semaphore.acquire(6);
                    assertEquals(5, semaphore.availablePermits());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        thread.start();

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(thread.isAlive());
                assertEquals(5, semaphore.availablePermits());
            }
        }, 5);
    }

    @Test
    public void testMultipleRelease() {
        int numberOfPermits = 20;

        for (int i = 0; i < numberOfPermits; i += 5) {
            assertEquals(i, semaphore.availablePermits());
            semaphore.release(5);
        }
        assertEquals(semaphore.availablePermits(), numberOfPermits);
    }

    @Test
    public void testMultipleRelease_whenNegative() {
        semaphore.init(0);

        try {
            semaphore.release(-5);
            fail();

        } catch (IllegalArgumentException expected) {
        }
        assertEquals(0, semaphore.availablePermits());
    }

    @Test
    public void testMultipleRelease_whenBlockedAcquireThreads() throws Exception {
        int permits = 10;
        semaphore.init(permits);
        semaphore.acquire(permits);

        Future future = spawn(new Runnable() {
            @Override
            public void run() {
                try {
                    semaphore.acquire();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        semaphore.release();
        future.get();
    }

    @Test
    public void testDrain() throws InterruptedException {
        int numberOfPermits = 20;

        assertTrue(semaphore.init(numberOfPermits));
        semaphore.acquire(5);
        int drainedPermits = semaphore.drainPermits();
        assertEquals(drainedPermits, numberOfPermits - 5);
        assertEquals(semaphore.availablePermits(), 0);
    }

    @Test
    public void testDrain_whenNoPermits() {
        semaphore.init(0);
        assertEquals(0, semaphore.drainPermits());
    }

    @Test
    public void testReduce() {
        int numberOfPermits = 20;

        assertTrue(semaphore.init(numberOfPermits));
        for (int i = 0; i < numberOfPermits; i += 5) {
            assertEquals(numberOfPermits - i, semaphore.availablePermits());
            semaphore.reducePermits(5);
        }

        assertEquals(semaphore.availablePermits(), 0);
    }

    @Test
    public void testReduce_whenArgumentNegative() {
        try {
            semaphore.reducePermits(-5);
            fail();
        } catch (IllegalArgumentException expected) {
        }
        assertEquals(0, semaphore.availablePermits());
    }

    @Test
    public void testIncrease_whenArgumentNegative() {
        try {
            semaphore.increasePermits(-5);
            fail();
        } catch (IllegalArgumentException expected) {
        }
        assertEquals(0, semaphore.availablePermits());
    }


    @Test
    public void testTryAcquire() {
        int numberOfPermits = 20;

        assertTrue(semaphore.init(numberOfPermits));
        for (int i = 0; i < numberOfPermits; i++) {
            assertEquals(numberOfPermits - i, semaphore.availablePermits());
            assertEquals(semaphore.tryAcquire(), true);
        }
        assertFalse(semaphore.tryAcquire());
        assertEquals(semaphore.availablePermits(), 0);
    }

    @Test
    public void testTryAcquireMultiple() {
        int numberOfPermits = 20;

        assertTrue(semaphore.init(numberOfPermits));
        for (int i = 0; i < numberOfPermits; i += 5) {
            assertEquals(numberOfPermits - i, semaphore.availablePermits());
            assertEquals(semaphore.tryAcquire(5), true);
        }

        assertEquals(semaphore.availablePermits(), 0);
    }

    @Test
    public void testTryAcquireMultiple_whenArgumentNegative() {
        int negativePermits = -5;
        semaphore.init(0);
        try {
            semaphore.tryAcquire(negativePermits);
            fail();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
        assertEquals(0, semaphore.availablePermits());
    }

    @Test
    public void testTryAcquire_whenNotEnoughPermits() throws InterruptedException {
        int numberOfPermits = 10;
        semaphore.init(numberOfPermits);
        semaphore.acquire(10);
        boolean result = semaphore.tryAcquire(1);

        assertFalse(result);
        assertEquals(0, semaphore.availablePermits());
    }

    @Test
    public void testInit_whenNotIntialized() {
        boolean result = semaphore.init(2);

        assertTrue(result);
        assertEquals(2, semaphore.availablePermits());
    }

    @Test
    public void testInit_whenAlreadyIntialized() {
        semaphore.init(2);

        boolean result = semaphore.init(4);

        assertFalse(result);
        assertEquals(2, semaphore.availablePermits());
    }

    @Override
    protected Config createConfig(int groupSize, int metadataGroupSize) {
        Config config = super.createConfig(groupSize, metadataGroupSize);
        config.getRaftConfig().addGroupConfig(new RaftGroupConfig(groupName, groupSize));

        RaftSemaphoreConfig semaphoreConfig = new RaftSemaphoreConfig(name, groupName, false);
        config.addRaftSemaphoreConfig(semaphoreConfig);
        return config;
    }
}
