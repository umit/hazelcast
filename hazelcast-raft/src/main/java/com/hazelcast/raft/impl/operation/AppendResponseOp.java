package com.hazelcast.raft.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.impl.RaftService;
import com.hazelcast.raft.impl.dto.AppendResponse;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class AppendResponseOp extends AsyncRaftOp {

    private AppendResponse appendResponse;

    public AppendResponseOp() {
    }

    public AppendResponseOp(String name, AppendResponse appendResponse) {
        super(name);
        this.appendResponse = appendResponse;
    }

    @Override
    public void run() throws Exception {
        RaftService service = getService();
        service.handleAppendResponse(name, appendResponse);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(appendResponse);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        appendResponse = in.readObject();
    }
}
