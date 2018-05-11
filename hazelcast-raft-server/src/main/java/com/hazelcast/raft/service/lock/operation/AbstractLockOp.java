package com.hazelcast.raft.service.lock.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.service.lock.LockEndpoint;
import com.hazelcast.raft.service.lock.RaftLockService;

import java.io.IOException;
import java.util.UUID;

/**
 * TODO: Javadoc Pending...
 */
abstract class AbstractLockOp extends RaftOp {

    String name;
    long sessionId;
    long threadId;
    UUID invUid;

    AbstractLockOp() {
    }

    AbstractLockOp(String name, long sessionId, long threadId, UUID invUid) {
        this.name = name;
        this.sessionId = sessionId;
        this.threadId = threadId;
        this.invUid = invUid;
    }

    LockEndpoint getLockEndpoint() {
        return new LockEndpoint(sessionId, threadId);
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
        out.writeLong(invUid.getLeastSignificantBits());
        out.writeLong(invUid.getMostSignificantBits());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readUTF();
        sessionId = in.readLong();
        threadId = in.readLong();
        long least = in.readLong();
        long most = in.readLong();
        invUid = new UUID(most, least);
    }
}
