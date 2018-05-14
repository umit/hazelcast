package com.hazelcast.raft.service.lock.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.raft.service.lock.LockEndpoint;
import com.hazelcast.raft.service.lock.RaftLockService;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 */
public class GetLockCountOp extends RaftOp {

    private static int NO_SESSION = -1;

    private String name;
    private long sessionId = NO_SESSION;
    private long threadId;

    public GetLockCountOp() {
    }

    public GetLockCountOp(String name) {
        this.name = name;
    }

    public GetLockCountOp(String name, long sessionId, long threadId) {
        this.name = name;
        this.sessionId = sessionId;
        this.threadId = threadId;
    }

    @Override
    protected Object doRun(RaftGroupId groupId, long commitIndex) {
        RaftLockService service = getService();
        Tuple2<LockEndpoint, Integer> result = service.lockCount(groupId, name);

        if (sessionId != NO_SESSION) {
            LockEndpoint endpoint = new LockEndpoint(sessionId, threadId);
            return endpoint.equals(result.element1) ? result.element2 : 0;
        }
        return result.element2;
    }

    @Override
    public final String getServiceName() {
        return RaftLockService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(name);
        out.writeLong(sessionId);
        out.writeLong(threadId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readUTF();
        sessionId = in.readLong();
        threadId = in.readLong();
    }
}
