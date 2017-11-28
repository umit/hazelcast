package com.hazelcast.raft.impl.service;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.service.operation.AppendFailureResponseOp;
import com.hazelcast.raft.impl.service.operation.AppendRequestOp;
import com.hazelcast.raft.impl.service.operation.AppendSuccessResponseOp;
import com.hazelcast.raft.impl.service.operation.InstallSnapshotOp;
import com.hazelcast.raft.impl.service.operation.PreVoteRequestOp;
import com.hazelcast.raft.impl.service.operation.PreVoteResponseOp;
import com.hazelcast.raft.impl.service.operation.VoteRequestOp;
import com.hazelcast.raft.impl.service.operation.VoteResponseOp;
import com.hazelcast.raft.impl.service.proxy.CreateRaftGroupReplicatingOperation;

public final class RaftServiceDataSerializerHook implements DataSerializerHook {

    private static final int RAFT_DS_FACTORY_ID = -1002;
    private static final String RAFT_DS_FACTORY = "hazelcast.serialization.ds.raft.service";

    public static final int F_ID = FactoryIdHelper.getFactoryId(RAFT_DS_FACTORY, RAFT_DS_FACTORY_ID);

    public static final int VOTE_REQUEST_OP = 1;
    public static final int VOTE_RESPONSE_OP = 2;
    public static final int APPEND_REQUEST_OP = 3;
    public static final int APPEND_SUCCESS_RESPONSE_OP = 4;
    public static final int APPEND_FAILURE_RESPONSE_OP = 5;
    public static final int INSTALL_SNAPSHOT_OP = 6;
    public static final int PRE_VOTE_REQUEST_OP = 7;
    public static final int PRE_VOTE_RESPONSE_OP = 8;
    public static final int CREATE_RAFT_GROUP_OP = 9;
    public static final int CREATE_RAFT_GROUP_REPLICATING_OP = 10;
    public static final int GROUP_INFO = 11;
    public static final int GROUP_ID = 12;

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
                    case CREATE_RAFT_GROUP_REPLICATING_OP:
                        return new CreateRaftGroupReplicatingOperation();
                    case GROUP_INFO:
                        return new RaftGroupInfo();
                    case GROUP_ID:
                        return new RaftGroupId();
                }
                throw new IllegalArgumentException("Undefined type: " + typeId);
            }
        };
    }
}
