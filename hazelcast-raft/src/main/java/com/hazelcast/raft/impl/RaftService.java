package com.hazelcast.raft.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.RaftMember;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendResponse;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
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

import static com.hazelcast.util.ThreadUtil.createThreadName;

/**
 * TODO: Javadoc Pending...
 */
public class RaftService implements ManagedService, ConfigurableService<RaftConfig> {

    public static final String SERVICE_NAME = "hz:core:raft";
    static final String METADATA_RAFT = "METADATA";

    private final Map<String, RaftNode> nodes = new ConcurrentHashMap<String, RaftNode>();
    private final NodeEngine nodeEngine;
    private final ILogger logger;

    private volatile StripedExecutor executor;
    private volatile RaftConfig config;

    public RaftService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        Collection<RaftEndpoint> endpoints;
        try {
            endpoints = getEndpoints();
        } catch (UnknownHostException e) {
            throw new HazelcastException(e);
        }
        logger.warning("CP nodes: " + endpoints);
        RaftEndpoint localEndpoint = getLocalEndpoint(endpoints);
        if (localEndpoint == null) {
            logger.warning("We are not in CP nodes group :(");
            return;
        }

        String threadPoolName = createThreadName(nodeEngine.getHazelcastInstance().getName(), "raft");
        this.executor = new StripedExecutor(logger, threadPoolName, RuntimeAvailableProcessors.get(), Integer.MAX_VALUE);

        RaftNode node = new RaftNode(METADATA_RAFT, localEndpoint, endpoints, nodeEngine, executor);
        nodes.put(METADATA_RAFT, node);
        node.start();
    }

    private RaftEndpoint getLocalEndpoint(Collection<RaftEndpoint> endpoints) {
        for (RaftEndpoint endpoint : endpoints) {
            if (nodeEngine.getThisAddress().equals(endpoint.getAddress())) {
                return endpoint;
            }
        }
        return null;
    }

    private Collection<RaftEndpoint> getEndpoints() throws UnknownHostException {
        Collection<RaftMember> members = config.getMembers();
        Set<RaftEndpoint> endpoints = new HashSet<RaftEndpoint>(members.size());
        for (RaftMember member : members) {
            AddressUtil.AddressHolder addressHolder = AddressUtil.getAddressHolder(member.getAddress());
            Address address = new Address(addressHolder.getAddress(), addressHolder.getPort());
            address.setScopeId(addressHolder.getScopeId());
            endpoints.add(new RaftEndpoint(member.getId(), address));
        }
        return endpoints;
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
            logger.severe("RaftNode[" + name + "] does not exist to handle: " + request);
            return;
        }
        node.handleVoteRequest(request);
    }

    public void handleVoteResponse(String name, VoteResponse response) {
        RaftNode node = nodes.get(name);
        if (node == null) {
            logger.severe("RaftNode[" + name + "] does not exist to handle: " + response);
            return;
        }
        node.handleVoteResponse(response);
    }

    public void handleAppendEntries(String name, AppendRequest request) {
        RaftNode node = nodes.get(name);
        if (node == null) {
            logger.severe("RaftNode[" + name + "] does not exist to handle: " + request);
            return;
        }
        node.handleAppendRequest(request);
    }

    public void handleAppendResponse(String name, AppendResponse response) {
        RaftNode node = nodes.get(name);
        if (node == null) {
            logger.severe("RaftNode[" + name + "] does not exist to handle: " + response);
            return;
        }
        node.handleAppendResponse(response);
    }

    RaftNode getRaftNode(String name) {
        return nodes.get(name);
    }
}
