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

package com.hazelcast.raft.service.countdownlatch.proxy;

import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.service.countdownlatch.RaftCountDownLatchService;
import com.hazelcast.raft.service.countdownlatch.operation.AwaitOp;
import com.hazelcast.raft.service.countdownlatch.operation.CountDownOp;
import com.hazelcast.raft.service.countdownlatch.operation.GetRemainingCountOp;
import com.hazelcast.raft.service.countdownlatch.operation.GetRoundOp;
import com.hazelcast.raft.service.countdownlatch.operation.TrySetCountOp;
import com.hazelcast.raft.service.spi.operation.DestroyRaftObjectOp;
import com.hazelcast.util.UuidUtil;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * TODO: Javadoc Pending...
 */
public class RaftCountDownLatchProxy implements ICountDownLatch {

    private final RaftGroupId groupId;
    private final String name;
    private final RaftInvocationManager raftInvocationManager;

    public RaftCountDownLatchProxy(RaftInvocationManager invocationManager, RaftGroupId groupId, String name) {
        this.raftInvocationManager = invocationManager;
        this.groupId = groupId;
        this.name = name;
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        checkNotNull(unit);

        long timeoutMillis = Math.max(0, unit.toMillis(timeout));
        return raftInvocationManager.<Boolean>invoke(groupId, new AwaitOp(name, timeoutMillis)).join();
    }

    @Override
    public void countDown() {
        int round = raftInvocationManager.<Integer>invoke(groupId, new GetRoundOp(name)).join();
        UUID invocationUid = UuidUtil.newUnsecureUUID();
        for (;;) {
            try {
                raftInvocationManager.invoke(groupId, new CountDownOp(name, round, invocationUid)).join();
                return;
            } catch (OperationTimeoutException ignored) {
                // I can retry safely because my retry would be idempotent...
            }
        }
    }

    @Override
    public int getCount() {
        return raftInvocationManager.<Integer>invoke(groupId, new GetRemainingCountOp(name)).join();
    }

    @Override
    public boolean trySetCount(int count) {
        return raftInvocationManager.<Boolean>invoke(groupId, new TrySetCountOp(name, count)).join();
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return RaftCountDownLatchService.SERVICE_NAME;
    }

    @Override
    public void destroy() {
        raftInvocationManager.invoke(groupId, new DestroyRaftObjectOp(getServiceName(), name)).join();
    }

}
