package com.hazelcast.raft.impl;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendResponse;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.raft.impl.operation.AppendRequestOp;
import com.hazelcast.raft.impl.operation.AppendResponseOp;
import com.hazelcast.raft.impl.operation.VoteRequestOp;
import com.hazelcast.raft.impl.operation.VoteResponseOp;

public final class RaftDataSerializerHook implements DataSerializerHook {

    private static final int RAFT_DS_FACTORY_ID = -1001;
    private static final String RAFT_DS_FACTORY = "hazelcast.serialization.ds.raft";

    public static final int F_ID = FactoryIdHelper.getFactoryId(RAFT_DS_FACTORY, RAFT_DS_FACTORY_ID);

    public static final int VOTE_REQUEST = 1;
    public static final int VOTE_REQUEST_OP = 2;
    public static final int VOTE_RESPONSE = 3;
    public static final int VOTE_RESPONSE_OP = 4;
    public static final int APPEND_REQUEST = 5;
    public static final int APPEND_REQUEST_OP = 6;
    public static final int APPEND_RESPONSE = 7;
    public static final int APPEND_RESPONSE_OP = 8;
    public static final int LOG_ENTRY = 9;
    public static final int ENDPOINT = 10;

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
                    case VOTE_REQUEST_OP:
                        return new VoteRequestOp();
                    case VOTE_RESPONSE:
                        return new VoteResponse();
                    case VOTE_RESPONSE_OP:
                        return new VoteResponseOp();
                    case APPEND_REQUEST:
                        return new AppendRequest();
                    case APPEND_REQUEST_OP:
                        return new AppendRequestOp();
                    case APPEND_RESPONSE:
                        return new AppendResponse();
                    case APPEND_RESPONSE_OP:
                        return new AppendResponseOp();
                    case LOG_ENTRY:
                        return new LogEntry();
                    case ENDPOINT:
                        return new RaftEndpoint();
                }
                throw new IllegalArgumentException("Undefined type: " + typeId);
            }
        };
    }
}
