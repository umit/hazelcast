package com.hazelcast.raft.impl.service;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.service.operation.integration.AppendFailureResponseOp;
import com.hazelcast.raft.impl.service.operation.integration.AppendRequestOp;
import com.hazelcast.raft.impl.service.operation.integration.AppendSuccessResponseOp;
import com.hazelcast.raft.impl.service.operation.integration.InstallSnapshotOp;
import com.hazelcast.raft.impl.service.operation.integration.PreVoteRequestOp;
import com.hazelcast.raft.impl.service.operation.integration.PreVoteResponseOp;
import com.hazelcast.raft.impl.service.operation.integration.VoteRequestOp;
import com.hazelcast.raft.impl.service.operation.integration.VoteResponseOp;
import com.hazelcast.raft.impl.service.operation.metadata.CompleteDestroyRaftGroupsOperation;
import com.hazelcast.raft.impl.service.operation.metadata.CompleteRemoveEndpointOperation;
import com.hazelcast.raft.impl.service.operation.metadata.CreateRaftGroupOperation;
import com.hazelcast.raft.impl.service.operation.metadata.TriggerDestroyRaftGroupOperation;
import com.hazelcast.raft.impl.service.operation.metadata.TriggerRemoveEndpointOperation;
import com.hazelcast.raft.impl.service.proxy.CreateRaftGroupReplicateOperation;
import com.hazelcast.raft.impl.service.proxy.DefaultRaftGroupReplicateOperation;
import com.hazelcast.raft.impl.service.proxy.MembershipChangeReplicateOperation;

public final class RaftServiceDataSerializerHook implements DataSerializerHook {

    private static final int RAFT_DS_FACTORY_ID = -1002;
    private static final String RAFT_DS_FACTORY = "hazelcast.serialization.ds.raft.service";

    public static final int F_ID = FactoryIdHelper.getFactoryId(RAFT_DS_FACTORY, RAFT_DS_FACTORY_ID);

    public static final int GROUP_ID = 1;
    public static final int GROUP_INFO = 2;
    public static final int VOTE_REQUEST_OP = 3;
    public static final int VOTE_RESPONSE_OP = 4;
    public static final int APPEND_REQUEST_OP = 5;
    public static final int APPEND_SUCCESS_RESPONSE_OP = 6;
    public static final int APPEND_FAILURE_RESPONSE_OP = 7;
    public static final int INSTALL_SNAPSHOT_OP = 8;
    public static final int PRE_VOTE_REQUEST_OP = 9;
    public static final int PRE_VOTE_RESPONSE_OP = 10;
    public static final int DEFAULT_RAFT_GROUP_REPLICATE_OP = 11;
    public static final int CREATE_RAFT_GROUP_OP = 12;
    public static final int CREATE_RAFT_GROUP_REPLICATE_OP = 13;
    public static final int TRIGGER_DESTROY_RAFT_GROUP_OP = 14;
    public static final int COMPLETE_DESTROY_RAFT_GROUPS_OP = 15;
    public static final int TRIGGER_REMOVE_ENDPOINT_OP = 16;
    public static final int COMPLETE_REMOVE_ENDPOINT_OP = 17;
    public static final int MEMBERSHIP_CHANGE_REPLICATE_OP = 18;
    public static final int METADATA_SNAPSHOT = 19;
    public static final int LEAVING_RAFT_ENDPOINT_CTX = 20;

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
                    case GROUP_INFO:
                        return new RaftGroupInfo();
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
                    case INSTALL_SNAPSHOT_OP:
                        return new InstallSnapshotOp();
                    case PRE_VOTE_REQUEST_OP:
                        return new PreVoteRequestOp();
                    case PRE_VOTE_RESPONSE_OP:
                        return new PreVoteResponseOp();
                    case CREATE_RAFT_GROUP_OP:
                        return new CreateRaftGroupOperation();
                    case DEFAULT_RAFT_GROUP_REPLICATE_OP:
                        return new DefaultRaftGroupReplicateOperation();
                    case CREATE_RAFT_GROUP_REPLICATE_OP:
                        return new CreateRaftGroupReplicateOperation();
                    case TRIGGER_DESTROY_RAFT_GROUP_OP:
                        return new TriggerDestroyRaftGroupOperation();
                    case COMPLETE_DESTROY_RAFT_GROUPS_OP:
                        return new CompleteDestroyRaftGroupsOperation();
                    case TRIGGER_REMOVE_ENDPOINT_OP:
                        return new TriggerRemoveEndpointOperation();
                    case COMPLETE_REMOVE_ENDPOINT_OP:
                        return new CompleteRemoveEndpointOperation();
                    case MEMBERSHIP_CHANGE_REPLICATE_OP:
                        return new MembershipChangeReplicateOperation();
                    case METADATA_SNAPSHOT:
                        return new MetadataSnapshot();
                    case LEAVING_RAFT_ENDPOINT_CTX:
                        return new LeavingRaftEndpointContext();

                }
                throw new IllegalArgumentException("Undefined type: " + typeId);
            }
        };
    }
}
