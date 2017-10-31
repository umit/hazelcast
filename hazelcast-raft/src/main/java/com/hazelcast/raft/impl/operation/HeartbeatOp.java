package com.hazelcast.raft.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.impl.RaftService;
import com.hazelcast.raft.impl.dto.AppendRequest;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class HeartbeatOp extends AsyncRaftOp {

    private AppendRequest appendRequest;

    public HeartbeatOp() {
    }

    public HeartbeatOp(String name, AppendRequest appendRequest) {
        super(name);
        this.appendRequest = appendRequest;
    }

    @Override
    public void run() throws Exception {
        RaftService service = getService();
        service.handleHeartbeat(name, appendRequest);
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
