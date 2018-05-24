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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.raft.service.lock.LockEndpoint;
import com.hazelcast.raft.service.lock.RaftLockDataSerializerHook;
import com.hazelcast.raft.service.lock.RaftLockService;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 */
public class GetLockCountOp extends RaftOp implements IdentifiedDataSerializable {

    private static int NO_SESSION = -1;

    private String name;
    private long sessionId = NO_SESSION;
    private long threadId;

    public GetLockCountOp() {
    }

    public GetLockCountOp(String name) {
        this.name = name;
    }

    public GetLockCountOp(String name, long sessionId, long threadId) {
        this.name = name;
        this.sessionId = sessionId;
        this.threadId = threadId;
    }

    @Override
    protected Object doRun(RaftGroupId groupId, long commitIndex) {
        RaftLockService service = getService();
        Tuple2<LockEndpoint, Integer> result = service.lockCount(groupId, name);

        if (sessionId != NO_SESSION) {
            LockEndpoint endpoint = new LockEndpoint(sessionId, threadId);
            return endpoint.equals(result.element1) ? result.element2 : 0;
        }
        return result.element2;
    }

    @Override
    public final String getServiceName() {
        return RaftLockService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return RaftLockDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftLockDataSerializerHook.GET_LOCK_COUNT_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(name);
        out.writeLong(sessionId);
        out.writeLong(threadId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readUTF();
        sessionId = in.readLong();
        threadId = in.readLong();
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", name=").append(name);
        sb.append(", sessionId=").append(sessionId);
        sb.append(", threadId=").append(threadId);
    }
}
