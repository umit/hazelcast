package com.hazelcast.raft.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendResponse;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.raft.impl.operation.AppendResponseOp;
import com.hazelcast.raft.impl.operation.VoteResponseOp;
import com.hazelcast.spi.ConfigurableService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
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

    public void handleVoteRequest(String name, VoteRequest request) {
        RaftNode node = nodes.get(name);
        if (node == null) {
            Operation op = new VoteResponseOp(name, new VoteResponse(request.term, false, nodeEngine.getThisAddress()));
            nodeEngine.getOperationService().send(op, request.candidate);
            return;
        }
        node.handleVoteRequest(request);
    }

    public void handleVoteResponse(String name, VoteResponse response) {
        RaftNode node = nodes.get(name);
        if (node == null) {
            logger.severe("RaftNode[" + name + "] doesn't exist!");
            return;
        }
        node.handleVoteResponse(response);
    }

    public void handleAppendEntries(String name, AppendRequest request) {
        RaftNode node = nodes.get(name);
        if (node == null) {
            // TODO: ?
            AppendResponseOp op = new AppendResponseOp(name, new AppendResponse(false, request.term, nodeEngine.getThisAddress(), 0));
            nodeEngine.getOperationService().send(op, request.leader);
            return;
        }
        node.handleAppendRequest(request);
    }

    public void handleAppendResponse(String name, AppendResponse response) {
        RaftNode node = nodes.get(name);
        if (node == null) {
            logger.severe("RaftNode[" + name + "] doesn't exist!");
            return;
        }
        node.handleAppendResponse(response);
    }

    public RaftNode getRaftNode(String name) {
        return nodes.get(name);
    }
}
