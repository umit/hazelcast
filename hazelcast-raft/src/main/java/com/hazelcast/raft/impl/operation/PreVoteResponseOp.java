package com.hazelcast.raft.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.impl.RaftDataSerializerHook;
import com.hazelcast.raft.impl.dto.PreVoteResponse;
import com.hazelcast.raft.impl.service.RaftService;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class PreVoteResponseOp extends AsyncRaftOp {

    private PreVoteResponse voteResponse;

    public PreVoteResponseOp() {
    }

    public PreVoteResponseOp(String name, PreVoteResponse voteResponse) {
        super(name);
        this.voteResponse = voteResponse;
    }

    @Override
    public void run() throws Exception {
        RaftService service = getService();
        service.handlePreVoteResponse(name, voteResponse);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(voteResponse);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        voteResponse = in.readObject();
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.PRE_VOTE_RESPONSE_OP;
    }
}
