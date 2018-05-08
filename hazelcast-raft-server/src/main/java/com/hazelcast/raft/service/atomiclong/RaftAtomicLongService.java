package com.hazelcast.raft.service.atomiclong;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.raft.service.atomiclong.proxy.RaftAtomicLongProxy;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftAtomicLongService implements ManagedService, SnapshotAwareService<Map<String, Long>> {

    public static final String SERVICE_NAME = "hz:raft:atomicLongService";

    private final Map<Tuple2<RaftGroupId, String>, RaftAtomicLong> map = new ConcurrentHashMap<Tuple2<RaftGroupId, String>, RaftAtomicLong>();
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
    public Map<String, Long> takeSnapshot(RaftGroupId groupId, long commitIndex) {
        checkNotNull(groupId);
        Map<String, Long> longs = new HashMap<String, Long>();
        for (RaftAtomicLong atomicLong : map.values()) {
            if (atomicLong.groupId().equals(groupId)) {
                longs.put(atomicLong.name(), atomicLong.value());
            }
        }

        return longs;
    }

    @Override
    public void restoreSnapshot(RaftGroupId groupId, long commitIndex, Map<String, Long> snapshot) {
        checkNotNull(groupId);
        for (Map.Entry<String, Long> e : snapshot.entrySet()) {
            String name = e.getKey();
            long val = e.getValue();
            map.put(Tuple2.of(groupId, name), new RaftAtomicLong(groupId, name, val));
        }
    }

    // TODO: in config, nodeCount or failure tolerance ?
    public IAtomicLong createNew(String longName, int nodeCount) throws ExecutionException, InterruptedException {
        RaftInvocationManager invocationManager = raftService.getInvocationManager();
        RaftGroupId groupId = invocationManager.createRaftGroup(SERVICE_NAME, nodeCount);
        return new RaftAtomicLongProxy(groupId, longName, invocationManager);
    }

    public IAtomicLong newProxy(RaftGroupId groupId, String name) {
        checkNotNull(groupId);
        checkNotNull(name);
        return new RaftAtomicLongProxy(groupId, name, raftService.getInvocationManager());
    }

    public RaftAtomicLong getAtomicLong(RaftGroupId groupId, String name) {
        checkNotNull(groupId);
        checkNotNull(name);
        Tuple2<RaftGroupId, String> key = Tuple2.of(groupId, name);
        RaftAtomicLong atomicLong = map.get(key);
        if (atomicLong == null) {
            atomicLong = new RaftAtomicLong(groupId, groupId.name());
            map.put(key, atomicLong);
        }
        return atomicLong;
    }
}
