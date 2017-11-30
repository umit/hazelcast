package com.hazelcast.raft.service.atomiclong;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.service.RaftGroupId;
import com.hazelcast.raft.service.atomiclong.proxy.RaftAtomicLongProxy;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.raft.impl.service.util.RaftGroupHelper.createRaftGroup;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftAtomicLongService implements ManagedService, SnapshotAwareService<Long> {

    public static final String SERVICE_NAME = "hz:raft:atomicLongService";
    public static final String PREFIX = "atomiclong:";

    private final Map<String, RaftAtomicLong> map = new ConcurrentHashMap<String, RaftAtomicLong>();
    private volatile NodeEngine nodeEngine;

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
    }

    @Override
    public Long takeSnapshot(String raftName, int commitIndex) {
        RaftAtomicLong atomicLong = map.get(raftName);
        assert atomicLong != null : "Name: " + raftName;
        assert atomicLong.commitIndex() == commitIndex : "Value: " + atomicLong + ", Commit-Index: " + commitIndex;
        return atomicLong.value();
    }

    @Override
    public void restoreSnapshot(String raftName, int commitIndex, Long snapshot) {
        RaftAtomicLong atomicLong = new RaftAtomicLong(raftName, snapshot, commitIndex);
        map.put(raftName, atomicLong);
    }

    public static String nameWithoutPrefix(String raftName) {
        assert raftName.startsWith(PREFIX) : "Raft-Name: " + raftName;
        return raftName.substring(PREFIX.length());
    }

    // TODO: in config, nodeCount or failure tolerance ?
    public IAtomicLong createNew(String name, int nodeCount) throws ExecutionException, InterruptedException {
        RaftGroupId groupId = createRaftGroup(nodeEngine, SERVICE_NAME, PREFIX + name, nodeCount);
        return new RaftAtomicLongProxy(groupId, nodeEngine);
    }

    public RaftAtomicLong getAtomicLong(RaftGroupId groupId) {
        RaftAtomicLong atomicLong = map.get(groupId.name());
        if (atomicLong == null) {
            atomicLong = new RaftAtomicLong(groupId.name());
            map.put(groupId.name(), atomicLong);
        }
        return atomicLong;
    }
}
