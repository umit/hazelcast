package com.hazelcast.raft.service.lock.operation;

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.lock.RaftLockService;

import java.util.UUID;

/**
 * TODO: Javadoc Pending...
 */
public class UnlockOp extends AbstractLockOp {

    public UnlockOp() {
    }

    public UnlockOp(String name, long sessionId, long threadId, UUID invUid) {
        super(name, sessionId, threadId, invUid);
    }

    @Override
    protected Object doRun(RaftGroupId groupId, long commitIndex) {
        RaftLockService service = getService();
        service.release(groupId, name, getLockEndpoint(), invUid);
        return true;
    }
}
