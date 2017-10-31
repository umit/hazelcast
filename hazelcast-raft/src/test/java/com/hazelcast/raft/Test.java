package com.hazelcast.raft;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.spi.properties.GroupProperty;

import java.util.Arrays;

/**
 * TODO: Javadoc Pending...
 *
 * @author mdogan 30.10.2017
 */
public class Test {

    static {
        System.setProperty(GroupProperty.LOGGING_TYPE.getName(), "log4j2");
        System.setProperty(GroupProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
    }

    public static void main(String[] args) {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).clear().addMember("127.0.0.1");

        RaftConfig raftConfig = new RaftConfig();
        raftConfig.setAddresses(Arrays.asList("127.0.0.1:5701", "127.0.0.1:5702"));

        config.getServicesConfig().addServiceConfig(
                new ServiceConfig().setEnabled(true).setName(RaftService.SERVICE_NAME)
                        .setClassName(RaftService.class.getName())
                        .setConfigObject(raftConfig));

        Hazelcast.newHazelcastInstance(config);
    }
}
