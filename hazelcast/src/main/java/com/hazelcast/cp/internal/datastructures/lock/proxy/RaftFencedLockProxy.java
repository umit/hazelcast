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

package com.hazelcast.cp.internal.datastructures.lock.proxy;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.FencedLock;
import com.hazelcast.cp.internal.datastructures.lock.operation.LockOp;
import com.hazelcast.cp.internal.session.ProxySessionManagerService;
import com.hazelcast.cp.internal.datastructures.spi.operation.DestroyRaftObjectOp;
import com.hazelcast.cp.internal.datastructures.lock.RaftLockOwnershipState;
import com.hazelcast.cp.internal.datastructures.lock.operation.ForceUnlockOp;
import com.hazelcast.cp.internal.datastructures.lock.operation.GetLockOwnershipStateOp;
import com.hazelcast.cp.internal.datastructures.lock.operation.TryLockOp;
import com.hazelcast.cp.internal.datastructures.lock.operation.UnlockOp;
import com.hazelcast.spi.InternalCompletableFuture;

import java.util.UUID;

/**
 * Server-side proxy of Raft-based {@link FencedLock} API
 */
public class RaftFencedLockProxy extends AbstractRaftFencedLockProxy {

    private final RaftInvocationManager invocationManager;

    public RaftFencedLockProxy(RaftInvocationManager invocationManager, ProxySessionManagerService sessionManager,
                               CPGroupId groupId, String name) {
        super(sessionManager, groupId, name);
        this.invocationManager = invocationManager;
    }

    @Override
    protected final InternalCompletableFuture<RaftLockOwnershipState> doLock(CPGroupId groupId, String name,
                                                                             long sessionId, long threadId,
                                                                             UUID invocationUid) {
        return invoke(new LockOp(name, sessionId, threadId, invocationUid));
    }

    @Override
    protected final InternalCompletableFuture<RaftLockOwnershipState> doTryLock(CPGroupId groupId, String name,
                                                                                long sessionId, long threadId,
                                                                                UUID invocationUid, long timeoutMillis) {
        return invoke(new TryLockOp(name, sessionId, threadId, invocationUid, timeoutMillis));
    }

    @Override
    protected final InternalCompletableFuture<Object> doUnlock(CPGroupId groupId, String name,
                                                               long sessionId, long threadId,
                                                               UUID invocationUid, int releaseCount) {
        return invoke(new UnlockOp(name, sessionId, threadId, invocationUid, releaseCount));
    }

    @Override
    protected final InternalCompletableFuture<Object> doForceUnlock(CPGroupId groupId, String name,
                                                                    UUID invocationUid, long expectedFence) {
        return invoke(new ForceUnlockOp(name, expectedFence, invocationUid));
    }

    @Override
    protected final InternalCompletableFuture<RaftLockOwnershipState> doGetLockOwnershipState(CPGroupId groupId,
                                                                                              String name) {
        return invoke(new GetLockOwnershipStateOp(name));
    }

    private <T> InternalCompletableFuture<T> invoke(RaftOp op) {
        return invocationManager.invoke(groupId, op);
    }

    @Override
    public void destroy() {
        invocationManager.invoke(groupId, new DestroyRaftObjectOp(getServiceName(), name)).join();
    }

}
