/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal;

import com.hazelcast.cp.internal.operation.ChangeRaftGroupMembershipOp;
import com.hazelcast.cp.internal.operation.DefaultRaftReplicateOp;
import com.hazelcast.cp.internal.operation.DestroyRaftGroupOp;
import com.hazelcast.cp.internal.operation.RaftQueryOp;
import com.hazelcast.cp.internal.operation.integration.AppendFailureResponseOp;
import com.hazelcast.cp.internal.operation.integration.AppendRequestOp;
import com.hazelcast.cp.internal.operation.integration.AppendSuccessResponseOp;
import com.hazelcast.cp.internal.operation.integration.InstallSnapshotOp;
import com.hazelcast.cp.internal.operation.integration.PreVoteRequestOp;
import com.hazelcast.cp.internal.operation.integration.PreVoteResponseOp;
import com.hazelcast.cp.internal.operation.integration.VoteRequestOp;
import com.hazelcast.cp.internal.operation.integration.VoteResponseOp;
import com.hazelcast.cp.internal.raftop.NotifyTermChangeOp;
import com.hazelcast.cp.internal.raftop.metadata.AddCPMemberOp;
import com.hazelcast.cp.internal.raftop.metadata.CompleteDestroyRaftGroupsOp;
import com.hazelcast.cp.internal.raftop.metadata.CompleteRaftGroupMembershipChangesOp;
import com.hazelcast.cp.internal.raftop.metadata.InitializeMetadataRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.CreateRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.CreateRaftNodeOp;
import com.hazelcast.cp.internal.raftop.metadata.DestroyRaftNodesOp;
import com.hazelcast.cp.internal.raftop.metadata.ForceDestroyRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.GetActiveCPMembersOp;
import com.hazelcast.cp.internal.raftop.metadata.GetActiveRaftGroupByNameOp;
import com.hazelcast.cp.internal.raftop.metadata.GetDestroyingRaftGroupIdsOp;
import com.hazelcast.cp.internal.raftop.metadata.GetInitialRaftGroupMembersIfCurrentGroupMemberOp;
import com.hazelcast.cp.internal.raftop.metadata.GetMembershipChangeContextOp;
import com.hazelcast.cp.internal.raftop.metadata.GetRaftGroupIdsOp;
import com.hazelcast.cp.internal.raftop.metadata.GetRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.RaftServicePreJoinOp;
import com.hazelcast.cp.internal.raftop.metadata.SendActiveCPMembersOp;
import com.hazelcast.cp.internal.raftop.metadata.TriggerDestroyRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.TriggerRemoveCPMemberOp;
import com.hazelcast.cp.internal.raftop.snapshot.RestoreSnapshotOp;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

@SuppressWarnings("checkstyle:declarationorder")
public final class RaftServiceDataSerializerHook implements DataSerializerHook {

    private static final int RAFT_DS_FACTORY_ID = -1002;
    private static final String RAFT_DS_FACTORY = "hazelcast.serialization.ds.raft.service";

    public static final int F_ID = FactoryIdHelper.getFactoryId(RAFT_DS_FACTORY, RAFT_DS_FACTORY_ID);

    public static final int GROUP_ID = 1;
    public static final int RAFT_GROUP_INFO = 2;
    public static final int PRE_VOTE_REQUEST_OP = 3;
    public static final int PRE_VOTE_RESPONSE_OP = 4;
    public static final int VOTE_REQUEST_OP = 5;
    public static final int VOTE_RESPONSE_OP = 6;
    public static final int APPEND_REQUEST_OP = 7;
    public static final int APPEND_SUCCESS_RESPONSE_OP = 8;
    public static final int APPEND_FAILURE_RESPONSE_OP = 9;
    public static final int METADATA_RAFT_GROUP_SNAPSHOT = 10;
    public static final int INSTALL_SNAPSHOT_OP = 11;
    public static final int DEFAULT_RAFT_GROUP_REPLICATE_OP = 12;
    public static final int CREATE_RAFT_GROUP_OP = 13;
    public static final int TRIGGER_DESTROY_RAFT_GROUP_OP = 14;
    public static final int COMPLETE_DESTROY_RAFT_GROUPS_OP = 15;
    public static final int TRIGGER_REMOVE_CP_MEMBER_OP = 16;
    public static final int COMPLETE_RAFT_GROUP_MEMBERSHIP_CHANGES_OP = 17;
    public static final int MEMBERSHIP_CHANGE_REPLICATE_OP = 18;
    public static final int MEMBERSHIP_CHANGE_CTX = 19;
    public static final int DEFAULT_RAFT_GROUP_QUERY_OP = 20;
    public static final int DESTROY_RAFT_NODES_OP = 21;
    public static final int GET_ACTIVE_CP_MEMBERS_OP = 22;
    public static final int GET_DESTROYING_RAFT_GROUP_IDS_OP = 23;
    public static final int GET_MEMBERSHIP_CHANGE_CONTEXT_OP = 24;
    public static final int GET_RAFT_GROUP_OP = 25;
    public static final int GET_ACTIVE_RAFT_GROUP_BY_NAME_OP = 26;
    public static final int CREATE_RAFT_NODE_OP = 27;
    public static final int DESTROY_RAFT_GROUP_OP = 28;
    public static final int RESTORE_SNAPSHOT_OP = 29;
    public static final int NOTIFY_TERM_CHANGE_OP = 30;
    public static final int CP_MEMBER = 31;
    public static final int SEND_ACTIVE_CP_MEMBERS_OP = 32;
    public static final int ADD_CP_MEMBER_OP = 33;
    public static final int INITIALIZE_METADATA_RAFT_GROUP_OP = 34;
    public static final int FORCE_DESTROY_RAFT_GROUP_OP = 35;
    public static final int GET_INITIAL_RAFT_GROUP_MEMBERS_IF_CURRENT_GROUP_MEMBER_OP = 36;
    public static final int GET_RAFT_GROUP_IDS_OP = 37;
    public static final int RAFT_PRE_JOIN_OP = 38;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {
            @Override
            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId) {
                    case GROUP_ID:
                        return new RaftGroupId();
                    case RAFT_GROUP_INFO:
                        return new CPGroupInfo();
                    case PRE_VOTE_REQUEST_OP:
                        return new PreVoteRequestOp();
                    case PRE_VOTE_RESPONSE_OP:
                        return new PreVoteResponseOp();
                    case VOTE_REQUEST_OP:
                        return new VoteRequestOp();
                    case VOTE_RESPONSE_OP:
                        return new VoteResponseOp();
                    case APPEND_REQUEST_OP:
                        return new AppendRequestOp();
                    case APPEND_SUCCESS_RESPONSE_OP:
                        return new AppendSuccessResponseOp();
                    case APPEND_FAILURE_RESPONSE_OP:
                        return new AppendFailureResponseOp();
                    case METADATA_RAFT_GROUP_SNAPSHOT:
                        return new MetadataRaftGroupSnapshot();
                    case INSTALL_SNAPSHOT_OP:
                        return new InstallSnapshotOp();
                    case CREATE_RAFT_GROUP_OP:
                        return new CreateRaftGroupOp();
                    case DEFAULT_RAFT_GROUP_REPLICATE_OP:
                        return new DefaultRaftReplicateOp();
                    case TRIGGER_DESTROY_RAFT_GROUP_OP:
                        return new TriggerDestroyRaftGroupOp();
                    case COMPLETE_DESTROY_RAFT_GROUPS_OP:
                        return new CompleteDestroyRaftGroupsOp();
                    case TRIGGER_REMOVE_CP_MEMBER_OP:
                        return new TriggerRemoveCPMemberOp();
                    case COMPLETE_RAFT_GROUP_MEMBERSHIP_CHANGES_OP:
                        return new CompleteRaftGroupMembershipChangesOp();
                    case MEMBERSHIP_CHANGE_REPLICATE_OP:
                        return new ChangeRaftGroupMembershipOp();
                    case MEMBERSHIP_CHANGE_CTX:
                        return new MembershipChangeContext();
                    case DEFAULT_RAFT_GROUP_QUERY_OP:
                        return new RaftQueryOp();
                    case DESTROY_RAFT_NODES_OP:
                        return new DestroyRaftNodesOp();
                    case GET_ACTIVE_CP_MEMBERS_OP:
                        return new GetActiveCPMembersOp();
                    case GET_DESTROYING_RAFT_GROUP_IDS_OP:
                        return new GetDestroyingRaftGroupIdsOp();
                    case GET_MEMBERSHIP_CHANGE_CONTEXT_OP:
                        return new GetMembershipChangeContextOp();
                    case GET_RAFT_GROUP_OP:
                        return new GetRaftGroupOp();
                    case GET_ACTIVE_RAFT_GROUP_BY_NAME_OP:
                        return new GetActiveRaftGroupByNameOp();
                    case CREATE_RAFT_NODE_OP:
                        return new CreateRaftNodeOp();
                    case DESTROY_RAFT_GROUP_OP:
                        return new DestroyRaftGroupOp();
                    case RESTORE_SNAPSHOT_OP:
                        return new RestoreSnapshotOp();
                    case NOTIFY_TERM_CHANGE_OP:
                        return new NotifyTermChangeOp();
                    case CP_MEMBER:
                        return new CPMemberInfo();
                    case SEND_ACTIVE_CP_MEMBERS_OP:
                        return new SendActiveCPMembersOp();
                    case ADD_CP_MEMBER_OP:
                        return new AddCPMemberOp();
                    case INITIALIZE_METADATA_RAFT_GROUP_OP:
                        return new InitializeMetadataRaftGroupOp();
                    case FORCE_DESTROY_RAFT_GROUP_OP:
                        return new ForceDestroyRaftGroupOp();
                    case GET_INITIAL_RAFT_GROUP_MEMBERS_IF_CURRENT_GROUP_MEMBER_OP:
                        return new GetInitialRaftGroupMembersIfCurrentGroupMemberOp();
                    case GET_RAFT_GROUP_IDS_OP:
                        return new GetRaftGroupIdsOp();
                    case RAFT_PRE_JOIN_OP:
                        return new RaftServicePreJoinOp();
                    default:
                        throw new IllegalArgumentException("Undefined type: " + typeId);
                }
            }
        };
    }
}