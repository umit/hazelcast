/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.cluster.impl.operations.JoinOperation;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.internal.partition.PartitionRuntimeState;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.exception.TargetNotMemberException;

import java.io.IOException;

public final class PartitionStateOperation extends AbstractOperation
        implements MigrationCycleOperation, JoinOperation {

    private PartitionRuntimeState partitionState;
    private boolean sync;

    public PartitionStateOperation() {
    }

    public PartitionStateOperation(PartitionRuntimeState partitionState) {
        this(partitionState, false);
    }

    public PartitionStateOperation(PartitionRuntimeState partitionState, boolean sync) {
        this.partitionState = partitionState;
        this.sync = sync;
    }

    @Override
    public void run() {
        partitionState.setEndpoint(getCallerAddress());
        InternalPartitionServiceImpl partitionService = getService();
        partitionService.processPartitionRuntimeState(partitionState);
    }

    @Override
    public boolean returnsResponse() {
        return sync;
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    public String getServiceName() {
        return InternalPartitionService.SERVICE_NAME;
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException
                || throwable instanceof TargetNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        partitionState = new PartitionRuntimeState();
        partitionState.readData(in);
        sync = in.readBoolean();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        partitionState.writeData(out);
        out.writeBoolean(sync);
    }
}
