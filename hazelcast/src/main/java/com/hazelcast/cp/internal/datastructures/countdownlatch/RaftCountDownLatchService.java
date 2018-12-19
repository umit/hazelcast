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

package com.hazelcast.cp.internal.datastructures.countdownlatch;

import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.util.Tuple2;
import com.hazelcast.cp.internal.datastructures.spi.blocking.AbstractBlockingService;
import com.hazelcast.cp.internal.datastructures.countdownlatch.proxy.RaftCountDownLatchProxy;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

import java.util.Collection;
import java.util.UUID;

import static com.hazelcast.cp.internal.RaftService.getObjectNameForProxy;

/**
 * Contains Raft-based count down latch instances
 */
public class RaftCountDownLatchService
        extends AbstractBlockingService<AwaitInvocationKey, RaftCountDownLatch, RaftCountDownLatchRegistry> {

    /**
     * Name of the service
     */
    public static final String SERVICE_NAME = "hz:raft:countDownLatchService";

    public RaftCountDownLatchService(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    @Override
    public ICountDownLatch createRaftObjectProxy(String name) {
        try {
            RaftService service = nodeEngine.getService(RaftService.SERVICE_NAME);
            CPGroupId groupId = service.createRaftGroupForProxy(name);
            return new RaftCountDownLatchProxy(raftService.getInvocationManager(), groupId, getObjectNameForProxy(name));
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public boolean trySetCount(CPGroupId groupId, String name, int count) {
        return getOrInitRegistry(groupId).trySetCount(name, count);
    }

    public int countDown(CPGroupId groupId, String name, int expectedRound, UUID invocationUuid) {
        RaftCountDownLatchRegistry registry = getOrInitRegistry(groupId);
        Tuple2<Integer, Collection<AwaitInvocationKey>> t = registry.countDown(name, expectedRound, invocationUuid);
        notifyWaitKeys(groupId, t.element2, true);

        return t.element1;
    }

    public boolean await(CPGroupId groupId, String name, long commitIndex, long timeoutMillis) {
        boolean success = getOrInitRegistry(groupId).await(name, commitIndex, timeoutMillis);
        if (!success) {
            scheduleTimeout(groupId, new AwaitInvocationKey(name, commitIndex), timeoutMillis);
        }

        return success;
    }

    public int getRemainingCount(CPGroupId groupId, String name) {
        return getOrInitRegistry(groupId).getRemainingCount(name);
    }

    public int getRound(CPGroupId groupId, String name) {
        return getOrInitRegistry(groupId).getRound(name);
    }

    @Override
    protected RaftCountDownLatchRegistry createNewRegistry(CPGroupId groupId) {
        return new RaftCountDownLatchRegistry(groupId);
    }

    @Override
    protected Object expiredWaitKeyResponse() {
        return false;
    }

    @Override
    protected String serviceName() {
        return SERVICE_NAME;
    }
}
