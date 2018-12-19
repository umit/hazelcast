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

package com.hazelcast.cp.internal.datastructures.lock.operation;

import com.hazelcast.cp.FencedLock;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.datastructures.lock.RaftLockDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.lock.RaftLockService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.UUID;

/**
 * Operation for {@link FencedLock#forceUnlock()}
 *
 * @see com.hazelcast.cp.internal.datastructures.lock.RaftLock#forceRelease(long, UUID)
 */
public class ForceUnlockOp extends RaftOp implements IndeterminateOperationStateAware, IdentifiedDataSerializable {

    private String name;
    private long expectedFence;
    private UUID invocationUid;

    public ForceUnlockOp() {
    }

    public ForceUnlockOp(String name, long expectedFence, UUID invocationUid) {
        this.name = name;
        this.expectedFence = expectedFence;
        this.invocationUid = invocationUid;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        RaftLockService service = getService();
        service.forceRelease(groupId, name, expectedFence, invocationUid);
        return true;
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
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
        return RaftLockDataSerializerHook.FORCE_UNLOCK_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeLong(expectedFence);
        out.writeLong(invocationUid.getLeastSignificantBits());
        out.writeLong(invocationUid.getMostSignificantBits());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        expectedFence = in.readLong();
        long least = in.readLong();
        long most = in.readLong();
        invocationUid = new UUID(most, least);
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", name=").append(name)
          .append(", expectedFence=").append(expectedFence)
          .append(", invocationUid=").append(invocationUid);

    }
}
