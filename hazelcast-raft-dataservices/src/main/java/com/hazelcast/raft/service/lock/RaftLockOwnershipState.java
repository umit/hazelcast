package com.hazelcast.raft.service.lock;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

import static com.hazelcast.raft.service.lock.RaftLockService.INVALID_FENCE;

public class RaftLockOwnershipState implements IdentifiedDataSerializable {

    public static final RaftLockOwnershipState NOT_LOCKED
            = new RaftLockOwnershipState(INVALID_FENCE, 0, -1, -1);

    private long fence;

    private int lockCount;

    private long sessionId;

    private long threadId;

    public RaftLockOwnershipState() {
    }

    public RaftLockOwnershipState(long fence, int lockCount, long sessionId, long threadId) {
        this.fence = fence;
        this.lockCount = lockCount;
        this.sessionId = sessionId;
        this.threadId = threadId;
    }

    public boolean isLocked() {
        return fence != INVALID_FENCE;
    }

    public long getFence() {
        return fence;
    }

    public int getLockCount() {
        return lockCount;
    }

    public long getSessionId() {
        return sessionId;
    }

    public long getThreadId() {
        return threadId;
    }

    @Override
    public int getFactoryId() {
        return RaftLockDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftLockDataSerializerHook.RAFT_LOCK_OWNERSHIP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(fence);
        out.writeInt(lockCount);
        out.writeLong(sessionId);
        out.writeLong(threadId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        fence = in.readLong();
        lockCount = in.readInt();
        sessionId = in.readLong();
        threadId = in.readLong();
    }
}
