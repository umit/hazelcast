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

package com.hazelcast.cp.internal.raft.impl.command;

import com.hazelcast.core.EndpointIdentifier;
import com.hazelcast.cp.internal.raft.MembershipChangeType;
import com.hazelcast.cp.internal.raft.command.RaftGroupCmd;
import com.hazelcast.cp.internal.raft.impl.RaftDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashSet;

/**
 * A {@code RaftGroupCmd} to update members of an existing Raft group.
 * This command is generated as a result of member add or remove request.
 */
public class ApplyRaftGroupMembersCmd extends RaftGroupCmd implements IdentifiedDataSerializable {

    private Collection<EndpointIdentifier> members;
    private EndpointIdentifier member;
    private MembershipChangeType changeType;

    public ApplyRaftGroupMembersCmd() {
    }

    public ApplyRaftGroupMembersCmd(Collection<EndpointIdentifier> members, EndpointIdentifier member, MembershipChangeType changeType) {
        this.members = members;
        this.member = member;
        this.changeType = changeType;
    }

    public Collection<EndpointIdentifier> getMembers() {
        return members;
    }

    public EndpointIdentifier getMember() {
        return member;
    }

    public MembershipChangeType getChangeType() {
        return changeType;
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.APPLY_RAFT_GROUP_MEMBERS_COMMAND;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(members.size());
        for (EndpointIdentifier member : members) {
            out.writeObject(member);
        }
        out.writeObject(member);
        out.writeUTF(changeType.name());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int count = in.readInt();
        Collection<EndpointIdentifier> members = new LinkedHashSet<EndpointIdentifier>();
        for (int i = 0; i < count; i++) {
            EndpointIdentifier member = in.readObject();
            members.add(member);
        }
        this.members = members;
        this.member = in.readObject();
        this.changeType = MembershipChangeType.valueOf(in.readUTF());
    }

    @Override
    public String toString() {
        return "ApplyRaftGroupMembersCmd{" + "members=" + members + ", member=" + member + ", changeType=" + changeType + '}';
    }
}
