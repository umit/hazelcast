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

import com.hazelcast.config.raft.RaftCountDownLatchConfig;
import com.hazelcast.config.raft.RaftGroupConfig;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.raft.service.blocking.AbstractBlockingService;
import com.hazelcast.raft.service.countdownlatch.proxy.RaftCountDownLatchProxy;
import com.hazelcast.raft.service.spi.RaftRemoteService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.util.ExceptionUtil;

import java.util.Collection;
import java.util.UUID;

/**
 * TODO: Javadoc Pending...
 */
public class RaftCountDownLatchService
        extends AbstractBlockingService<CountDownLatchInvocationKey, RaftCountDownLatch, CountDownLatchRegistry>
        implements SnapshotAwareService<CountDownLatchRegistrySnapshot>, RaftRemoteService {

    public static final String SERVICE_NAME = "hz:raft:countDownLatchService";

    public RaftCountDownLatchService(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    @Override
    protected CountDownLatchRegistry createNewRegistry(RaftGroupId groupId) {
        return new CountDownLatchRegistry(groupId);
    }

    @Override
    protected Object invalidatedResult() {
        return false;
    }

    @Override
    protected String serviceName() {
        return SERVICE_NAME;
    }

    @Override
    public CountDownLatchRegistrySnapshot takeSnapshot(RaftGroupId groupId, long commitIndex) {
        CountDownLatchRegistry registry = getResourceRegistryOrNull(groupId);
        return registry != null ? registry.toSnapshot() : null;
    }

    @Override
    public void restoreSnapshot(RaftGroupId groupId, long commitIndex, CountDownLatchRegistrySnapshot snapshot) {
        if (snapshot != null) {
            CountDownLatchRegistry registry = getOrInitResourceRegistry(groupId);
            registry.restore(snapshot);
        }
    }

    @Override
    public ICountDownLatch createRaftObjectProxy(String name) {
        try {
            RaftGroupId groupId = createRaftGroup(name).get();
            return new RaftCountDownLatchProxy(raftService.getInvocationManager(), groupId, name);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public ICompletableFuture<RaftGroupId> createRaftGroup(String name) {
        String raftGroupRef = getRaftGroupRef(name);

        RaftInvocationManager invocationManager = raftService.getInvocationManager();
        return invocationManager.createRaftGroup(raftGroupRef);
    }

    private String getRaftGroupRef(String name) {
        RaftCountDownLatchConfig config = getConfig(name);
        return config != null ? config.getRaftGroupRef() : RaftGroupConfig.DEFAULT_GROUP;
    }

    private RaftCountDownLatchConfig getConfig(String name) {
        return nodeEngine.getConfig().findRaftCountDownLatchConfig(name);
    }

    @Override
    public boolean destroyRaftObject(RaftGroupId groupId, String name) {
        CountDownLatchRegistry registry = getOrInitResourceRegistry(groupId);
        Collection<Long> indices = registry.destroyResource(name);
        if (indices == null) {
            return false;
        }
        completeFutures(groupId, indices, new DistributedObjectDestroyedException("Lock[" + name + "] is destroyed"));
        return true;
    }

    public boolean trySetCount(RaftGroupId groupId, String name, int count) {
        CountDownLatchRegistry registry = getOrInitResourceRegistry(groupId);
        return registry.trySetCount(name, count);
    }

    public int countDown(RaftGroupId groupId, String name, int expectedRound, UUID invocationUuid) {
        CountDownLatchRegistry registry = getOrInitResourceRegistry(groupId);
        Tuple2<Integer, Collection<CountDownLatchInvocationKey>> t = registry.countDown(name, expectedRound, invocationUuid);
        notifyWaitKeys(groupId, t.element2, true);

        return t.element1;
    }

    public boolean await(RaftGroupId groupId, String name, long commitIndex, long timeoutMillis) {
        CountDownLatchRegistry registry = getOrInitResourceRegistry(groupId);
        return registry.await(name, commitIndex, timeoutMillis);
    }

    public int getRemainingCount(RaftGroupId groupId, String name) {
        CountDownLatchRegistry registry = getOrInitResourceRegistry(groupId);
        return registry.getRemainingCount(name);
    }

    public int getRound(RaftGroupId groupId, String name) {
        CountDownLatchRegistry registry = getOrInitResourceRegistry(groupId);
        return registry.getRound(name);
    }
}
