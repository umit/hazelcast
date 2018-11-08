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
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftMemberImpl;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.RaftMetadataManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.impl.service.proxy.InvocationTargetLeaveAware;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * TODO: Javadoc Pending...
 *
 */
public class CreateRaftGroupOp extends RaftOp implements InvocationTargetLeaveAware, IdentifiedDataSerializable {

    private String groupName;
    private Collection<RaftMemberImpl> members;

    public CreateRaftGroupOp() {
    }

    public CreateRaftGroupOp(String groupName, Collection<RaftMemberImpl> members) {
        this.groupName = groupName;
        this.members = members;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftService service = getService();
        RaftMetadataManager metadataManager = service.getMetadataManager();
        return metadataManager.createRaftGroup(groupName, members, commitIndex);
    }

    @Override
    public boolean isSafeToRetryOnTargetLeave() {
        return true;
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(groupName);
        out.writeInt(members.size());
        for (RaftMemberImpl member : members) {
            out.writeObject(member);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        groupName = in.readUTF();
        int len = in.readInt();
        members = new ArrayList<RaftMemberImpl>(len);
        for (int i = 0; i < len; i++) {
            RaftMemberImpl member = in.readObject();
            members.add(member);
        }
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.CREATE_RAFT_GROUP_OP;
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", groupName=").append(groupName)
          .append(", members=").append(members);
    }
}
