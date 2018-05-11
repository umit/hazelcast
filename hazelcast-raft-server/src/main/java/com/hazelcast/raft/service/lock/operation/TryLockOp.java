package com.hazelcast.raft.service.lock.operation;

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.lock.RaftLockService;

import java.util.UUID;

/**
 * TODO: Javadoc Pending...
 */
public class TryLockOp extends AbstractLockOp {

    public TryLockOp() {
    }

    public TryLockOp(String name, long sessionId, long threadId, UUID invUid) {
        super(name, sessionId, threadId, invUid);
    }

    @Override
    protected Object doRun(RaftGroupId groupId, long commitIndex) {
        RaftLockService service = getService();
        return service.acquire(groupId, name, getLockEndpoint(), commitIndex, invUid, false);
    }
}
