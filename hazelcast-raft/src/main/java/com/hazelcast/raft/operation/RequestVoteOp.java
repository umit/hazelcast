package com.hazelcast.raft.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftService;
import com.hazelcast.raft.VoteRequest;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 * @author mdogan 30.10.2017
 */
public class RequestVoteOp extends AsyncRaftOp {

    private VoteRequest voteRequest;

    public RequestVoteOp() {
    }

    public RequestVoteOp(String name, VoteRequest voteRequest) {
        super(name);
        this.voteRequest = voteRequest;
    }

    @Override
    public void run() throws Exception {
        RaftService service = getService();
        service.handleRequestVote(name, voteRequest, newResponseHandler());
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
}
