package com.hazelcast.raft.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.RaftService;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class AppendEntriesOp extends AsyncRaftOp {

    private AppendRequest appendRequest;

    public AppendEntriesOp() {
    }

    public AppendEntriesOp(String name, AppendRequest appendRequest) {
        super(name);
        this.appendRequest = appendRequest;
    }

    @Override
    public void run() throws Exception {
        RaftService service = getService();
        service.handleAppendEntries(name, appendRequest, newResponseHandler());
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(appendRequest);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        appendRequest = in.readObject();
    }
}
