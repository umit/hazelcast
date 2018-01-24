package com.hazelcast.raft.service.lock.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.operation.RaftOperation;
import com.hazelcast.raft.service.lock.RaftLockService;

import java.io.IOException;
import java.util.UUID;

/**
 * TODO: Javadoc Pending...
 */
abstract class AbstractLockOperation extends RaftOperation {

    RaftGroupId groupId;
    String uid;
    long threadId;
    UUID invUid;

    public AbstractLockOperation() {
    }

    public AbstractLockOperation(RaftGroupId groupId, String uid, long threadId, UUID invUid) {
        this.groupId = groupId;
        this.uid = uid;
        this.threadId = threadId;
        this.invUid = invUid;
    }

    @Override
    public final String getServiceName() {
        return RaftLockService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(groupId);
        out.writeUTF(uid);
        out.writeLong(threadId);
        out.writeLong(invUid.getLeastSignificantBits());
        out.writeLong(invUid.getMostSignificantBits());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        groupId = in.readObject();
        uid = in.readUTF();
        threadId = in.readLong();
        long least = in.readLong();
        long most = in.readLong();
        invUid = new UUID(most, least);
    }
}
