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

package com.hazelcast.raft.service.session;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.session.SessionResponse;
import com.hazelcast.raft.impl.session.operation.CreateSessionOp;
import com.hazelcast.raft.impl.session.operation.HeartbeatSessionOp;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.TimeUnit;

/**
 * TODO: Javadoc Pending...
 */
public class SessionManagerService extends AbstractSessionManager {

    public static String SERVICE_NAME = "hz:raft:sessionManager";

    private final NodeEngine nodeEngine;

    public SessionManagerService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    private RaftInvocationManager getInvocationManager() {
        RaftService raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
        return raftService.getInvocationManager();
    }

    @Override
    protected void scheduleTask(Runnable task, long period, TimeUnit unit) {
        nodeEngine.getExecutionService().scheduleWithRepetition(task, period, period, unit);
    }

    @Override
    protected SessionResponse requestNewSession(RaftGroupId groupId) {
        ICompletableFuture<SessionResponse> future = getInvocationManager().invoke(groupId, new CreateSessionOp());
        try {
            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    protected ICompletableFuture<Object> heartbeat(RaftGroupId groupId, long sessionId) {
        return getInvocationManager().invoke(groupId, new HeartbeatSessionOp(sessionId));
    }
}
