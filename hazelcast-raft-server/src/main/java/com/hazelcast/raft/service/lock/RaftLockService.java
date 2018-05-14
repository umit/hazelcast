package com.hazelcast.raft.service.lock;

import com.hazelcast.config.raft.RaftGroupConfig;
import com.hazelcast.config.raft.RaftLockConfig;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.ILock;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.session.SessionExpiredException;
import com.hazelcast.raft.impl.session.SessionAccessor;
import com.hazelcast.raft.impl.session.SessionAwareService;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.raft.service.lock.proxy.RaftLockProxy;
import com.hazelcast.raft.service.session.SessionManagerService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * TODO: Javadoc Pending...
 */
public class RaftLockService implements ManagedService, SnapshotAwareService, SessionAwareService {

    public static final String SERVICE_NAME = "hz:raft:lockService";

    private final ConcurrentMap<Tuple2<RaftGroupId, String>, RaftLock> locks = new ConcurrentHashMap<Tuple2<RaftGroupId, String>, RaftLock>();
    private final NodeEngine nodeEngine;
    private volatile RaftService raftService;
    private volatile SessionAccessor sessionAccessor;

    public RaftLockService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

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
        // TODO fixit
        return null;
    }

    @Override
    public void restoreSnapshot(RaftGroupId raftGroupId, long commitIndex, Object snapshot) {
        // TODO fixit
    }

    @Override
    public void setSessionAccessor(SessionAccessor accessor) {
        this.sessionAccessor = accessor;
    }

    @Override
    public void invalidateSession(RaftGroupId groupId, long sessionId) {
        for (Map.Entry<Tuple2<RaftGroupId, String>, RaftLock> entry : locks.entrySet()) {
            if (groupId.equals(entry.getKey().element1)) {
                RaftLock lock = entry.getValue();
                LockEndpoint owner = lock.owner();
                if (owner != null && sessionId == owner.sessionId) {
                    Collection<Long> indices = lock.release(owner, Integer.MAX_VALUE, null);
                    notifyWaiters(groupId, indices);
                }
            }
        }
    }

    public ILock createNew(String name) throws ExecutionException, InterruptedException {
        RaftGroupId groupId = createNewAsync(name).get();
        SessionManagerService sessionManager = nodeEngine.getService(SessionManagerService.SERVICE_NAME);
        return new RaftLockProxy(name, groupId, sessionManager, raftService.getInvocationManager());
    }

    public ICompletableFuture<RaftGroupId> createNewAsync(String name) {
        RaftLockConfig config = getConfig(name);
        checkNotNull(config);

        RaftInvocationManager invocationManager = raftService.getInvocationManager();
        RaftGroupConfig groupConfig = config.getRaftGroupConfig();
        if (groupConfig != null) {
            return invocationManager.createRaftGroup(groupConfig);
        } else {
            return invocationManager.createRaftGroup(config.getRaftGroupRef());
        }
    }

    private RaftLockConfig getConfig(String name) {
        return nodeEngine.getConfig().findRaftLockConfig(name);
    }

    public RaftLockProxy newProxy(String name, RaftGroupId groupId, String sessionId) {
        throw new UnsupportedOperationException();
    }

    private RaftLock getRaftLock(RaftGroupId groupId, String name) {
        checkNotNull(groupId);
        checkNotNull(name);
        Tuple2<RaftGroupId, String> key = Tuple2.of(groupId, name);
        RaftLock raftLock = locks.get(key);
        if (raftLock == null) {
            raftLock = new RaftLock(groupId, name);
            locks.put(key, raftLock);
        }
        return raftLock;
    }

    public boolean acquire(RaftGroupId groupId, String name, LockEndpoint endpoint, long commitIndex, UUID invUid, boolean wait) {
        if (sessionAccessor.isValid(groupId, endpoint.sessionId)) {
            sessionAccessor.heartbeat(groupId, endpoint.sessionId);
            RaftLock raftLock = getRaftLock(groupId, name);
            return raftLock.acquire(endpoint, commitIndex, invUid, wait);
        }
        throw new SessionExpiredException(endpoint.sessionId);
    }

    public void release(RaftGroupId groupId, String name, LockEndpoint endpoint, UUID invUid) {
        if (sessionAccessor.isValid(groupId, endpoint.sessionId)) {
            sessionAccessor.heartbeat(groupId, endpoint.sessionId);
            RaftLock raftLock = getRaftLock(groupId, name);
            Collection<Long> indices = raftLock.release(endpoint, invUid);
            notifyWaiters(groupId, indices);
        } else {
            throw new IllegalMonitorStateException();
        }
    }

    private void notifyWaiters(RaftGroupId groupId, Collection<Long> indices) {
        if (!indices.isEmpty()) {
            RaftNodeImpl raftNode = (RaftNodeImpl) raftService.getRaftNode(groupId);
            for (Long index : indices) {
                raftNode.completeFuture(index, true);
            }
        }
    }

    public Tuple2<LockEndpoint, Integer> lockCount(RaftGroupId groupId, String name) {
        checkNotNull(groupId);
        checkNotNull(name);
        RaftLock raftLock = locks.get(Tuple2.of(groupId, name));
        if (raftLock == null) {
            return Tuple2.of(null, 0);
        }
        return raftLock.lockCount();
    }
}
