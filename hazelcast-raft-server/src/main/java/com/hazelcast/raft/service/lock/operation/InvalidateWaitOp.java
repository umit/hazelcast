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

package com.hazelcast.raft.service.lock.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.lock.LockEndpoint;
import com.hazelcast.raft.service.lock.LockInvocationKey;
import com.hazelcast.raft.service.lock.RaftLockDataSerializerHook;
import com.hazelcast.raft.service.lock.RaftLockService;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 */
public class InvalidateWaitOp extends AbstractLockOp {

    private long invocationCommitIndex;

    public InvalidateWaitOp() {
    }

    public InvalidateWaitOp(LockInvocationKey key) {
        super(key.name, key.endpoint.sessionId, key.endpoint.threadId, key.invocationUid);
        this.invocationCommitIndex = key.commitIndex;
    }

    @Override
    protected Object doRun(RaftGroupId groupId, long commitIndex) {
        RaftLockService service = getService();
        LockEndpoint endpoint = getLockEndpoint();
        LockInvocationKey key = new LockInvocationKey(name, getLockEndpoint(), invocationCommitIndex, invocationUid);
        service.invalidateWait(groupId, key);
        return null;
    }

    @Override
    public int getId() {
        return RaftLockDataSerializerHook.INVALIDATE_WAIT_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeLong(invocationCommitIndex);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        invocationCommitIndex = in.readLong();
    }
}
