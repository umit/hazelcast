package com.hazelcast.raft.service.lock.operation;

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.lock.LockEndpoint;
import com.hazelcast.raft.service.lock.RaftLockService;

import java.util.UUID;

/**
 * TODO: Javadoc Pending...
 */
public class TryLockOperation extends AbstractLockOperation {

    public TryLockOperation() {
    }

    public TryLockOperation(RaftGroupId groupId, String uid, long threadId, UUID invUid) {
        super(groupId, uid, threadId, invUid);
    }

    @Override
    protected Object doRun(long commitIndex) {
        RaftLockService service = getService();
        LockEndpoint endpoint = new LockEndpoint(uid, threadId);
        return service.acquire(groupId, endpoint, commitIndex, invUid, false);
    }
}
