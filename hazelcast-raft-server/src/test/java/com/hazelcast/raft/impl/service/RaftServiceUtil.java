package com.hazelcast.raft.impl.service;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.raft.impl.RaftNode;

import static com.hazelcast.test.HazelcastTestSupport.getNodeEngineImpl;

public class RaftServiceUtil {

    public static RaftService getRaftService(HazelcastInstance instance) {
        return getNodeEngineImpl(instance).getService(RaftService.SERVICE_NAME);
    }

    public static RaftNode getRaftNode(HazelcastInstance instance, RaftGroupId groupId) {
        return getRaftService(instance).getRaftNode(groupId);
    }

    public static RaftGroupInfo getRaftGroupInfo(HazelcastInstance instance, RaftGroupId groupId) {
        return getRaftService(instance).getRaftGroupInfo(groupId);
    }
}
