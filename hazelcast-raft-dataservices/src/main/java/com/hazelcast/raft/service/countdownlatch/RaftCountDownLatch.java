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
import com.hazelcast.raft.service.blocking.BlockingResource;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static com.hazelcast.util.Preconditions.checkTrue;
import static java.lang.Math.max;

/**
 * TODO: Javadoc Pending...
 */
public class RaftCountDownLatch extends BlockingResource<CountDownLatchInvocationKey> {

    private int round;
    private int countDownFrom;
    private final Set<UUID> countDownUids = new HashSet<UUID>();

    RaftCountDownLatch(RaftGroupId groupId, String name) {
        super(groupId, name);
    }

    RaftCountDownLatch(RaftCountDownLatchSnapshot snapshot) {
        super(snapshot.getGroupId(), snapshot.getName());
        this.waitKeys.addAll(snapshot.getWaitKeys());
        this.round = snapshot.getRound();
        this.countDownFrom = snapshot.getCountDownFrom();
        this.countDownUids.addAll(snapshot.getCountDownUids());
    }

    @Override
    protected void onInvalidateSession(long sessionId, Long2ObjectHashMap<Object> result) {
    }

    Tuple2<Integer, Collection<CountDownLatchInvocationKey>> countDown(int expectedRound, UUID invocationUuid) {
        if (expectedRound > round) {
            throw new IllegalArgumentException();
        }

        if (expectedRound < round) {
            Collection<CountDownLatchInvocationKey> c = Collections.emptyList();
            return Tuple2.of(0, c);
        }

        countDownUids.add(invocationUuid);
        int remaining = getRemainingCount();
        if (remaining > 0) {
            Collection<CountDownLatchInvocationKey> c = Collections.emptyList();
            return Tuple2.of(remaining, c);
        }

        return Tuple2.of(0, getWaitKeys());
    }

    boolean trySetCount(int count) {
        if (getRemainingCount() > 0) {
            return false;
        }

        checkTrue(count > 0, "cannot set non-positive count: " + count);

        this.countDownFrom = count;
        round++;
        countDownUids.clear();
        return true;
    }

    boolean await(long commitIndex, boolean wait) {
        boolean success = (getRemainingCount() == 0);
        if (!success && wait) {
            waitKeys.add(new CountDownLatchInvocationKey(name, commitIndex));
        }

        return success;
    }

    int getRound() {
        return round;
    }

    int getRemainingCount() {
        return max(0, countDownFrom - countDownUids.size());
    }

    RaftCountDownLatchSnapshot toSnapshot() {
        return new RaftCountDownLatchSnapshot(groupId, name, waitKeys, round, countDownFrom, countDownUids);
    }

}
