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

package com.hazelcast.raft.service.countdownlatch;

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.raft.service.blocking.ResourceRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * TODO: Javadoc Pending...
 */
public class CountDownLatchRegistry extends ResourceRegistry<CountDownLatchInvocationKey, RaftCountDownLatch> {

    CountDownLatchRegistry(RaftGroupId groupId) {
        super(groupId);
    }

    @Override
    protected RaftCountDownLatch createNewResource(RaftGroupId groupId, String name) {
        return new RaftCountDownLatch(groupId, name);
    }

    boolean trySetCount(String name, int count) {
        RaftCountDownLatch latch = getOrInitResource(name);
        return latch.trySetCount(count);
    }

    Tuple2<Integer, Collection<CountDownLatchInvocationKey>> countDown(String name, int expectedRound, UUID invocationUuid) {
        RaftCountDownLatch latch = getOrInitResource(name);
        return latch.countDown(expectedRound, invocationUuid);
    }

    boolean await(String name, long commitIndex, long timeoutMs) {
        RaftCountDownLatch latch = getOrInitResource(name);
        boolean success = latch.await(commitIndex, timeoutMs > 0);
        if (!success) {
            scheduleWaitTimeout(new CountDownLatchInvocationKey(name, commitIndex), timeoutMs);
        }

        return success;
    }

    int getRemainingCount(String name) {
        RaftCountDownLatch latch = getOrInitResource(name);
        return latch.getRemainingCount();
    }

    int getRound(String name) {
        RaftCountDownLatch latch = getOrInitResource(name);
        return latch.getRound();
    }

    CountDownLatchRegistrySnapshot toSnapshot() {
        List<RaftCountDownLatchSnapshot> latchSnapshots = new ArrayList<RaftCountDownLatchSnapshot>();
        for (RaftCountDownLatch latch : resources.values()) {
            latchSnapshots.add(latch.toSnapshot());
        }
        return new CountDownLatchRegistrySnapshot(latchSnapshots, destroyedNames);
    }

    void restore(CountDownLatchRegistrySnapshot snapshot) {
        for (RaftCountDownLatchSnapshot latchSnapshot : snapshot.getLatches()) {
            resources.put(latchSnapshot.getName(), new RaftCountDownLatch(latchSnapshot));
        }

        destroyedNames.addAll(snapshot.getDestroyedLatchNames());
    }

}
