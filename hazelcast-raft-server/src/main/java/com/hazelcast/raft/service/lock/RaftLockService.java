package com.hazelcast.raft.service.lock;

import com.hazelcast.core.ILock;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.raft.service.lock.proxy.RaftLockProxy;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

/**
 * TODO: Javadoc Pending...
 */
public class RaftLockService implements ManagedService, SnapshotAwareService {

    public static final String SERVICE_NAME = "hz:raft:lockService";
    public static final String PREFIX = "lock:";

    private final ConcurrentMap<RaftGroupId, RaftLock> locks = new ConcurrentHashMap<RaftGroupId, RaftLock>();
    private volatile RaftService raftService;

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
    }

    @Override
    public void reset() {

    }

    @Override
    public void shutdown(boolean terminate) {

    }

    @Override
    public Object takeSnapshot(RaftGroupId raftGroupId, long commitIndex) {
        return null;
    }

    @Override
    public void restoreSnapshot(RaftGroupId raftGroupId, long commitIndex, Object snapshot) {

    }

    public static String nameWithoutPrefix(String raftName) {
        assert raftName.startsWith(PREFIX) : "Raft-Name: " + raftName;
        return raftName.substring(PREFIX.length());
    }

    public ILock createNew(String name, int replicas, String uid) throws ExecutionException, InterruptedException {
        RaftInvocationManager invocationManager = raftService.getInvocationManager();
        RaftGroupId groupId = invocationManager.createRaftGroup(SERVICE_NAME, PREFIX + name, replicas);
        return new RaftLockProxy(groupId, invocationManager, uid);
    }

    public RaftLockProxy newProxy(RaftGroupId groupId, String uid) {
        return new RaftLockProxy(groupId, raftService.getInvocationManager(), uid);
    }

    private RaftLock getRaftLock(RaftGroupId groupId) {
        RaftLock raftLock = locks.get(groupId);
        if (raftLock == null) {
            raftLock = new RaftLock(groupId.name());
            locks.put(groupId, raftLock);
        }
        return raftLock;
    }

    public boolean acquire(RaftGroupId groupId, LockEndpoint endpoint, long commitIndex, UUID invUid, boolean wait) {
        RaftLock raftLock = getRaftLock(groupId);
        return raftLock.acquire(endpoint, commitIndex, invUid, wait);
    }

    public void release(RaftGroupId groupId, LockEndpoint endpoint, UUID invUid) {
        RaftLock raftLock = getRaftLock(groupId);
        Collection<Long> indices = raftLock.release(endpoint, invUid);
        if (!indices.isEmpty()) {
            RaftNodeImpl raftNode = (RaftNodeImpl) raftService.getRaftNode(groupId);
            for (Long index : indices) {
                raftNode.completeFuture(index, true);
            }
        }
    }

    public Tuple2<LockEndpoint, Integer> lockCount(RaftGroupId groupId) {
        RaftLock raftLock = locks.get(groupId);
        if (raftLock == null) {
            return new Tuple2<LockEndpoint, Integer>(null, 0);
        }
        return raftLock.lockCount();
    }
}
