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

package com.hazelcast.raft.service.atomicref.operation;

import com.hazelcast.core.IFunction;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.atomicref.AtomicReferenceDataSerializerHook;
import com.hazelcast.raft.service.atomicref.RaftAtomicRef;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * TODO: Javadoc Pending...
 */
public class ApplyOp extends AbstractAtomicRefOp implements IdentifiedDataSerializable {

    public enum RETURN_VALUE_TYPE {
        NO_RETURN_VALUE,
        RETURN_PREVIOUS_VALUE,
        RETURN_NEW_VALUE
    }

    private Data function;
    private RETURN_VALUE_TYPE returnValueType;
    private boolean alter;

    public ApplyOp() {
    }

    public ApplyOp(String name, Data function, RETURN_VALUE_TYPE returnValueType, boolean alter) {
        super(name);
        checkNotNull(function);
        checkNotNull(returnValueType);
        this.function = function;
        this.returnValueType = returnValueType;
        this.alter = alter;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftAtomicRef ref = getAtomicRef(groupId);
        Data currentData = ref.get();
        Data newData = callFunction(currentData);

        if (alter) {
            ref.set(newData);
        }

        if (returnValueType == RETURN_VALUE_TYPE.NO_RETURN_VALUE) {
            return null;
        }

        return returnValueType == RETURN_VALUE_TYPE.RETURN_PREVIOUS_VALUE ? currentData : newData;
    }

    private Data callFunction(Data currentData) {
        NodeEngine nodeEngine = getNodeEngine();
        IFunction func = nodeEngine.toObject(function);
        Object input = nodeEngine.toObject(currentData);
        //noinspection unchecked
        Object output = func.apply(input);
        return nodeEngine.toData(output);
    }

    @Override
    public int getId() {
        return AtomicReferenceDataSerializerHook.APPLY_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        super.writeData(out);
        out.writeData(function);
        out.writeUTF(returnValueType.name());
        out.writeBoolean(alter);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        super.readData(in);
        function = in.readData();
        returnValueType = RETURN_VALUE_TYPE.valueOf(in.readUTF());
        alter = in.readBoolean();
    }
}
