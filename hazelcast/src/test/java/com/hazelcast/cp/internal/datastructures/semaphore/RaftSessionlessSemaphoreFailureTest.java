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

package com.hazelcast.cp.internal.datastructures.semaphore;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.cp.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.atomiclong.operation.AddAndGetOp;
import com.hazelcast.cp.internal.datastructures.semaphore.proxy.RaftSessionlessSemaphoreProxy;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.ConstructorFunction;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.cp.internal.datastructures.semaphore.proxy.GloballyUniqueThreadIdUtil.GLOBAL_THREAD_ID_GENERATOR_NAME;
import static com.hazelcast.cp.internal.datastructures.semaphore.proxy.GloballyUniqueThreadIdUtil.getGlobalThreadId;
import static com.hazelcast.cp.internal.session.AbstractSessionManager.NO_SESSION_ID;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftSessionlessSemaphoreFailureTest extends RaftSemaphoreFailureTest {

    @Override
    RaftGroupId getGroupId(ISemaphore semaphore) {
        return ((RaftSessionlessSemaphoreProxy) semaphore).getGroupId();
    }

    @Override
    boolean isStrictModeEnabled() {
        return false;
    }

    @Override
    long getSessionId(HazelcastInstance semaphoreInstance, RaftGroupId groupId) {
        return NO_SESSION_ID;
    }

    @Override
    long getThreadId(final HazelcastInstance semaphoreInstance, RaftGroupId groupId) {
        return getGlobalThreadId(groupId, new ConstructorFunction<RaftGroupId, Long>() {
            @Override
            public Long createNew(RaftGroupId groupId) {
                InternalCompletableFuture<Long> f = getRaftInvocationManager(semaphoreInstance)
                        .invoke(groupId, new AddAndGetOp(GLOBAL_THREAD_ID_GENERATOR_NAME, 1));
                return f.join();
            }
        });
    }

}
