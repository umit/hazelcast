package com.hazelcast.raft.service.atomiclong;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.raft.impl.service.CreateRaftGroupHelper;
import com.hazelcast.raft.service.atomiclong.proxy.RaftAtomicLongProxy;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftAtomicLongService implements ManagedService {

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

    // TODO: in config, nodeCount or failure tolerance ?
    public IAtomicLong createNew(String name, int nodeCount) throws ExecutionException, InterruptedException {
        String raftName = PREFIX + name;
        CreateRaftGroupHelper.createRaftGroup(nodeEngine, SERVICE_NAME, raftName, nodeCount);
        return new RaftAtomicLongProxy(name, nodeEngine);
    }

    public RaftAtomicLong getAtomicLong(String name) {
        RaftAtomicLong atomicLong = map.get(name);
        if (atomicLong == null) {
            atomicLong = new RaftAtomicLong(name);
            map.put(name, atomicLong);
        }
        return atomicLong;
    }
}
