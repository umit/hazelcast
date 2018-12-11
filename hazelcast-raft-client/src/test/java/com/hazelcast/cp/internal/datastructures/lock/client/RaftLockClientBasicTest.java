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

package com.hazelcast.cp.internal.datastructures.lock.client;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.cp.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.lock.RaftLockBasicTest;
import com.hazelcast.cp.internal.datastructures.session.AbstractSessionManager;
import com.hazelcast.cp.internal.datastructures.session.SessionManagerProvider;
import com.hazelcast.cp.internal.datastructures.util.ClientAccessor;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftLockClientBasicTest extends RaftLockBasicTest {

    private HazelcastInstance client;

    @Override
    protected TestHazelcastInstanceFactory createTestFactory() {
        return new TestHazelcastFactory();
    }

    @Override
    protected HazelcastInstance[] createInstances() {
        HazelcastInstance[] instances = super.createInstances();
        TestHazelcastFactory f = (TestHazelcastFactory) factory;
        client = f.newHazelcastClient();
        return instances;
    }

    @Override
    protected ILock createLock(String name) {
        return RaftLockProxy.create(client, name);
    }

    @After
    public void shutdown() {
        factory.terminateAll();
    }

    @Override
    protected RaftGroupId getGroupId(ILock lock) {
        return ((RaftLockProxy) lock).getGroupId();
    }

    @Override
    protected AbstractSessionManager getSessionManager() {
        return SessionManagerProvider.get(ClientAccessor.getClient(client));
    }


}
