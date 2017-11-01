package com.hazelcast.raft.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.raft.RaftConfig;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * TODO: Javadoc Pending...
 *
 */
public class Test {

    static {
        System.setProperty(GroupProperty.LOGGING_TYPE.getName(), "log4j2");
        System.setProperty(GroupProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).clear().addMember("127.0.0.1");

        RaftConfig raftConfig = new RaftConfig();
        raftConfig.setAddresses(Arrays.asList("127.0.0.1:5701", "127.0.0.1:5702", "127.0.0.1:5703"));

        config.getServicesConfig().addServiceConfig(
                new ServiceConfig().setEnabled(true).setName(RaftService.SERVICE_NAME)
                        .setClassName(RaftService.class.getName())
                        .setConfigObject(raftConfig));

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);

        RaftService service = HazelcastTestSupport.getNodeEngineImpl(instance).getService(RaftService.SERVICE_NAME);
        RaftNode node = service.getRaftNode("METADATA");

        TimeUnit.SECONDS.sleep(10);

        for (int i = 0; i < 30; i++) {
            String s = String.valueOf(i);
            final Future replicate = node.replicate(s);
//            System.out.println("ERROR DONE " + replicate.get());
            Thread.sleep(5000);
        }
    }
}
