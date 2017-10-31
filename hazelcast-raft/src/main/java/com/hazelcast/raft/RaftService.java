package com.hazelcast.raft;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.operation.RaftResponseHandler;
import com.hazelcast.spi.ConfigurableService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.AddressUtil;
import com.hazelcast.util.executor.StripedExecutor;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TODO: Javadoc Pending...
 */
public class RaftService implements ManagedService, ConfigurableService<RaftConfig> {

    public static final String SERVICE_NAME = "hz:core:raft";
    private static final String METADATA_RAFT = "METADATA";

    private final Map<String, RaftNode> nodes = new ConcurrentHashMap<String, RaftNode>();
    private final StripedExecutor executor =
            new StripedExecutor(Logger.getLogger("RaftServiceExecutor"), "raft", RuntimeAvailableProcessors.get(), Integer.MAX_VALUE);

    private volatile ILogger logger;
    private volatile NodeEngine nodeEngine;
    private volatile RaftConfig config;

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        Collection<Address> addresses;
        try {
            addresses = getAddresses();
        } catch (UnknownHostException e) {
            throw new HazelcastException(e);
        }
        logger.warning("CP nodes: " + addresses);
        if (!addresses.contains(nodeEngine.getThisAddress())) {
            logger.warning("We are not in CP nodes group :(");
            return;
        }

        RaftNode node = new RaftNode(METADATA_RAFT, addresses, nodeEngine, executor);
        nodes.put(METADATA_RAFT, node);
        node.start();
    }

    private Collection<Address> getAddresses() throws UnknownHostException {
        Collection<String> endpoints = config.getAddresses();
        Set<Address> addresses = new HashSet<Address>(endpoints.size());
        for (String endpoint : endpoints) {
            AddressUtil.AddressHolder addressHolder = AddressUtil.getAddressHolder(endpoint);
            Address address = new Address(addressHolder.getAddress(), addressHolder.getPort());
            address.setScopeId(addressHolder.getScopeId());
            addresses.add(address);
        }
        return addresses;
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
        executor.shutdown();
    }

    @Override
    public void configure(RaftConfig config) {
        this.config = config;
    }

    public void handleRequestVote(String name, VoteRequest voteRequest, RaftResponseHandler responseHandler) {
        RaftNode node = nodes.get(name);
        if (node == null) {
           responseHandler.send(new VoteResponse(voteRequest.term, false, nodeEngine.getThisAddress()));
           return;
        }
        node.handleRequestVote(voteRequest, responseHandler);
    }
}
