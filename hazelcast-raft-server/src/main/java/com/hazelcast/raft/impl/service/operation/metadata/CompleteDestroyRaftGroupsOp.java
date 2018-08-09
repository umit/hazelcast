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

package com.hazelcast.raft.impl.service.operation.metadata;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.service.RaftMetadataManager;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * TODO: Javadoc Pending...
 */
public class CompleteDestroyRaftGroupsOp extends RaftOp implements IdentifiedDataSerializable {

    private Set<RaftGroupId> groupIds;

    public CompleteDestroyRaftGroupsOp() {
    }

    public CompleteDestroyRaftGroupsOp(Set<RaftGroupId> groupIds) {
        this.groupIds = groupIds;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftService service = getService();
        RaftMetadataManager metadataManager = service.getMetadataManager();
        metadataManager.completeDestroyRaftGroups(groupIds);
        return null;
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(groupIds.size());
        for (RaftGroupId groupId : groupIds) {
            out.writeObject(groupId);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int count = in.readInt();
        groupIds = new HashSet<RaftGroupId>();
        for (int i = 0; i < count; i++) {
            RaftGroupId groupId = in.readObject();
            groupIds.add(groupId);
        }
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.COMPLETE_DESTROY_RAFT_GROUPS_OP;
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", groupIds=").append(groupIds);
    }
}
