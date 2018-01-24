package com.hazelcast.raft.service.lock.operation;

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.lock.LockEndpoint;
import com.hazelcast.raft.service.lock.RaftLockService;

import java.util.UUID;

/**
 * TODO: Javadoc Pending...
 */
public class UnlockOperation extends AbstractLockOperation {

    public UnlockOperation() {
    }

    public UnlockOperation(RaftGroupId groupId, String uid, long threadId, UUID invUid) {
        super(groupId, uid, threadId, invUid);
    }

    @Override
    protected Object doRun(long commitIndex) {
        RaftLockService service = getService();
        LockEndpoint endpoint = new LockEndpoint(uid, threadId);
        service.release(groupId, endpoint, invUid);
        return true;
    }
}
