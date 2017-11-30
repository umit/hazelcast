package com.hazelcast.raft.impl;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.raft.impl.dto.InstallSnapshot;
import com.hazelcast.raft.impl.dto.PreVoteRequest;
import com.hazelcast.raft.impl.dto.PreVoteResponse;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.operation.RestoreSnapshotOp;
import com.hazelcast.raft.impl.operation.TerminateRaftGroupOp;

public final class RaftDataSerializerHook implements DataSerializerHook {

    private static final int RAFT_DS_FACTORY_ID = -1001;
    private static final String RAFT_DS_FACTORY = "hazelcast.serialization.ds.raft";

    public static final int F_ID = FactoryIdHelper.getFactoryId(RAFT_DS_FACTORY, RAFT_DS_FACTORY_ID);

    public static final int VOTE_REQUEST = 1;
    public static final int VOTE_RESPONSE = 2;
    public static final int APPEND_REQUEST = 3;
    public static final int APPEND_SUCCESS_RESPONSE = 4;
    public static final int APPEND_FAILURE_RESPONSE = 5;
    public static final int INSTALL_SNAPSHOT = 6;
    public static final int RESTORE_SNAPSHOT_OP = 7;
    public static final int LOG_ENTRY = 8;
    public static final int ENDPOINT = 9;
    public static final int PRE_VOTE_REQUEST = 10;
    public static final int PRE_VOTE_RESPONSE = 11;
    public static final int TERMINATE_RAFT_GROUP_OP = 12;

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
                    case VOTE_REQUEST:
                        return new VoteRequest();
                    case VOTE_RESPONSE:
                        return new VoteResponse();
                    case APPEND_REQUEST:
                        return new AppendRequest();
                    case APPEND_SUCCESS_RESPONSE:
                        return new AppendSuccessResponse();
                    case APPEND_FAILURE_RESPONSE:
                        return new AppendFailureResponse();
                    case INSTALL_SNAPSHOT:
                        return new InstallSnapshot();
                    case RESTORE_SNAPSHOT_OP:
                        return new RestoreSnapshotOp();
                    case LOG_ENTRY:
                        return new LogEntry();
                    case ENDPOINT:
                        return new RaftEndpoint();
                    case PRE_VOTE_REQUEST:
                        return new PreVoteRequest();
                    case PRE_VOTE_RESPONSE:
                        return new PreVoteResponse();
                    case TERMINATE_RAFT_GROUP_OP:
                        return new TerminateRaftGroupOp();
                }
                throw new IllegalArgumentException("Undefined type: " + typeId);
            }
        };
    }
}
