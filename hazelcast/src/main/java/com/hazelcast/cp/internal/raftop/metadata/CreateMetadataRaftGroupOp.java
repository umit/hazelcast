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

package com.hazelcast.cp.internal.raftop.metadata;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.CPMember;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.MetadataRaftGroupManager;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Used during cluster startup by CP members to commit their CP member list
 * to the Metadata group. in order to guarantee that each CP member discovers
 * the same list. Fails with {@link IllegalArgumentException} if a CP member
 * commits a different list.
 */
public class CreateMetadataRaftGroupOp extends RaftOp implements IndeterminateOperationStateAware, IdentifiedDataSerializable {

    private List<CPMember> initialMembers;
    private int metadataMembersCount;

    public CreateMetadataRaftGroupOp() {
    }

    public CreateMetadataRaftGroupOp(List<CPMember> initialMembers, int metadataMembersCount) {
        this.initialMembers = initialMembers;
        this.metadataMembersCount = metadataMembersCount;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        RaftService service = getService();
        MetadataRaftGroupManager metadataManager = service.getMetadataGroupManager();
        metadataManager.createInitialMetadataRaftGroup(initialMembers, metadataMembersCount);
        return null;
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.CREATE_METADATA_RAFT_GROUP_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(initialMembers.size());
        for (CPMember member : initialMembers) {
            out.writeObject(member);
        }
        out.writeInt(metadataMembersCount);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int len = in.readInt();
        initialMembers = new ArrayList<CPMember>(len);
        for (int i = 0; i < len; i++) {
            CPMember member = in.readObject();
            initialMembers.add(member);
        }
        metadataMembersCount = in.readInt();
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append("members=").append(metadataMembersCount);
    }
}
