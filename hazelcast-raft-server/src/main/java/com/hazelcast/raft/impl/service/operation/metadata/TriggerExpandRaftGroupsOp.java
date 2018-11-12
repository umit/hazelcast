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
import com.hazelcast.raft.impl.service.MetadataRaftGroupManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.impl.InvocationTargetLeaveAware;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Crashed CP nodes are removed from Raft groups via an explicit API call.
 * Then, number of members in the corresponding Raft groups will be smaller then their initial size.
 * When new CP nodes are added to the CP sub-system, these Raft groups are expanded by adding the given members.
 * <p/>
 * Fails with {@link IllegalArgumentException} if there is an ongoing membership change in the CP sub-system.
 * Fails with {@link IllegalArgumentException} if a given CP node is not in the current active CP node list.
 * Fails with {@link IllegalArgumentException} if a given CP node is not in the candidate list of its Raft group.
 * <p/>
 * This operation is committed to the Metadata group.
 */
public class TriggerExpandRaftGroupsOp extends RaftOp implements InvocationTargetLeaveAware, IdentifiedDataSerializable {

    private Map<RaftGroupId, RaftMemberImpl> membersToAdd;

    public TriggerExpandRaftGroupsOp() {
    }

    public TriggerExpandRaftGroupsOp(Map<RaftGroupId, RaftMemberImpl> membersToAdd) {
        this.membersToAdd = membersToAdd;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftService service = getService();
        MetadataRaftGroupManager metadataManager = service.getMetadataGroupManager();
        return metadataManager.triggerExpandRaftGroups(membersToAdd);
    }

    @Override
    public boolean isRetryableOnTargetLeave() {
        return false;
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
        return RaftServiceDataSerializerHook.TRIGGER_EXPAND_RAFT_GROUPS_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(membersToAdd.size());
        for (Entry<RaftGroupId, RaftMemberImpl> e : membersToAdd.entrySet()) {
            out.writeObject(e.getKey());
            out.writeObject(e.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int count = in.readInt();
        membersToAdd = new HashMap<RaftGroupId, RaftMemberImpl>();
        for (int i = 0; i < count; i++) {
            RaftGroupId groupId = in.readObject();
            RaftMemberImpl member = in.readObject();
            membersToAdd.put(groupId, member);
        }
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", membersToAdd=").append(membersToAdd);
    }
}
