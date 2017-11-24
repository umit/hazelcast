package com.hazelcast.raft.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.impl.RaftDataSerializerHook;
import com.hazelcast.raft.impl.dto.PreVoteRequest;
import com.hazelcast.raft.impl.service.RaftService;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class PreVoteRequestOp extends AsyncRaftOp {

    private PreVoteRequest voteRequest;

    public PreVoteRequestOp() {
    }

    public PreVoteRequestOp(String name, PreVoteRequest voteRequest) {
        super(name);
        this.voteRequest = voteRequest;
    }

    @Override
    public void run() throws Exception {
        RaftService service = getService();
        service.handlePreVoteRequest(name, voteRequest);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(voteRequest);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        voteRequest = in.readObject();
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.PRE_VOTE_REQUEST_OP;
    }
}
