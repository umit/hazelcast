/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.CPMemberInfo;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.util.ExceptionUtil;
import org.junit.Test;

import java.util.concurrent.Future;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public abstract class AbstractAtomicRegisterSnapshotTest<T> extends HazelcastRaftTestSupport {

    protected HazelcastInstance[] createInstances() {
        return newInstances(3, 3, 1);
    }

    protected abstract CPGroupId getGroupId();

    protected abstract T setAndGetInitialValue();

    protected abstract T readValue();

    @Test
    public void test() throws Exception {
        final T value = setAndGetInitialValue();

        Future future = spawn(new RestartCpMemberTask());

        while (!future.isDone()) {
            T v = readValue();
            assertEquals(value, v);
            LockSupport.parkNanos(100);
        }
        future.get();
    }

    public class RestartCpMemberTask implements Runnable {
        @Override
        public void run() {
            for (int i = 0; i < 5; i++) {
                try {
                    sleepSeconds(10);
                    HazelcastInstance[] instances = factory.getAllHazelcastInstances().toArray(new HazelcastInstance[0]);
                    HazelcastInstance instance = getLeaderInstance(instances, getGroupId());
                    CPMemberInfo cpMember = getRaftService(instance).getLocalCPMember();
                    assertNotNull(cpMember);
                    instance.getLifecycleService().shutdown();

                    instance = factory.newHazelcastInstance(cpMember.getAddress(), createConfig(3, 3));
                    instance.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember().get();
                } catch (Exception e) {
                    throw ExceptionUtil.rethrow(e);
                }
            }
        }
    }

}
