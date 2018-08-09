/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.service.countdownlatch.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.service.countdownlatch.RaftCountDownLatchDataSerializerHook;
import com.hazelcast.raft.service.countdownlatch.RaftCountDownLatchService;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 */
public abstract class AbstractCountDownLatchOp extends RaftOp implements IdentifiedDataSerializable {

    protected String name;

    public AbstractCountDownLatchOp() {
    }

    AbstractCountDownLatchOp(String name) {
        this.name = name;
    }

    @Override
    public final String getServiceName() {
        return RaftCountDownLatchService.SERVICE_NAME;
    }

    @Override
    public final int getFactoryId() {
        return RaftCountDownLatchDataSerializerHook.F_ID;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", name=").append(name);
    }
}
