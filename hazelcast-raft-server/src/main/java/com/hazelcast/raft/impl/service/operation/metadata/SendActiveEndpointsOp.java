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

package com.hazelcast.raft.impl.service.operation.metadata;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftEndpointImpl;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class SendActiveEndpointsOp extends Operation implements IdentifiedDataSerializable {

    private Collection<RaftEndpointImpl> endpoints;

    public SendActiveEndpointsOp() {
    }

    public SendActiveEndpointsOp(Collection<RaftEndpointImpl> endpoints) {
        this.endpoints = endpoints;
    }

    @Override
    public void run() {
        RaftService service = getService();
        service.getMetadataManager().setActiveEndpoints(endpoints);
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(endpoints.size());
        for (RaftEndpointImpl endpoint : endpoints) {
            out.writeObject(endpoint);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int len = in.readInt();
        endpoints = new ArrayList<RaftEndpointImpl>(len);
        for (int i = 0; i < len; i++) {
            RaftEndpointImpl endpoint = in.readObject();
            endpoints.add(endpoint);
        }
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.SEND_ACTIVE_ENDPOINTS_OP;
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", endpoints=").append(endpoints);
    }
}
