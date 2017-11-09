package com.hazelcast.raft.impl.testing;

import com.hazelcast.logging.Logger;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftUtil;
import com.hazelcast.util.executor.StripedExecutor;
import org.junit.Assert;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.hazelcast.raft.impl.RaftUtil.majority;
import static com.hazelcast.raft.impl.RaftUtil.minority;
import static com.hazelcast.raft.impl.RaftUtil.newRaftEndpoint;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableMap;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

public class RaftGroup {

    private final StripedExecutor stripedExecutor;
    private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService cachedExecutor = Executors.newCachedThreadPool();

    private final RaftEndpoint[] endpoints;
    private final LocalRaftIntegration[] integrations;
    private final RaftNode[] nodes;


    public RaftGroup(int size) {
        this(size, Collections.<String, Class>emptyMap());
    }

    public RaftGroup(int size, Map<String, Class> serviceRegistrations) {
        stripedExecutor = new StripedExecutor(Logger.getLogger("executor"), "test", size, Integer.MAX_VALUE);
        endpoints = new RaftEndpoint[size];
        integrations = new LocalRaftIntegration[size];

        for (int i = 0; i < size; i++) {
            endpoints[i] = newRaftEndpoint(5000 + i);
            Map<String, Object> services = new HashMap<String, Object>(serviceRegistrations.size());
            for (Map.Entry<String, Class> entry : serviceRegistrations.entrySet()) {
                try {
                    services.put(entry.getKey(), entry.getValue().newInstance());
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            }
            integrations[i] = new LocalRaftIntegration(endpoints[i], unmodifiableMap(services), scheduledExecutor, cachedExecutor);
        }

        nodes = new RaftNode[size];
        for (int i = 0; i < size; i++) {
            nodes[i] = new RaftNode("node-" + i, endpoints[i], asList(endpoints), integrations[i], stripedExecutor);
        }
    }

    public void start() {
        startWithoutDiscovery();
        initDiscovery();
    }

    public void startWithoutDiscovery() {
        for (RaftNode node : nodes) {
            node.start();
        }
    }

    public void initDiscovery() {
        for (LocalRaftIntegration integration : integrations) {
            for (RaftNode node : nodes) {
                if (!node.getLocalEndpoint().equals(integration.getLocalEndpoint())) {
                    integration.discoverNode(node);
                }
            }
        }
    }

    public RaftNode getNode(int index) {
        return nodes[index];
    }

    public RaftEndpoint getEndpoint(int index) {
        return endpoints[index];
    }

    public LocalRaftIntegration getIntegration(int index) {
        return integrations[index];
    }

    public void waitUntilLeaderElected() {
        for (RaftNode node : nodes) {
            RaftUtil.waitUntilLeaderElected(node);
        }
    }

    public RaftEndpoint getLeaderEndpoint() {
        RaftEndpoint leader = null;
        for (RaftNode node : nodes) {
            RaftEndpoint endpoint = RaftUtil.getLeaderEndpoint(node);
            if (leader == null) {
                leader = endpoint;
            } else if (!leader.equals(endpoint)) {
                throw new IllegalStateException("Group doesn't have a single leader endpoint yet!");
            }
        }
        return leader;
    }

    public RaftNode getLeaderNode() {
        RaftEndpoint leaderEndpoint = getLeaderEndpoint();
        if (leaderEndpoint == null) {
            return null;
        }
        for (RaftNode node : nodes) {
            if (leaderEndpoint.equals(node.getLocalEndpoint())) {
                return node;
            }
        }
        throw new AssertionError("Leader endpoint is " + leaderEndpoint + ", but leader node could not be found!");
    }

    public int getLeaderIndex() {
        RaftEndpoint leaderEndpoint = getLeaderEndpoint();
        if (leaderEndpoint == null) {
            return -1;
        }
        for (int i = 0; i < endpoints.length; i++) {
            if (leaderEndpoint.equals(endpoints[i])) {
                return i;
            }
        }
        throw new AssertionError("Leader endpoint is " + leaderEndpoint + ", but this endpoint is unknown to group!");
    }

    public int getIndexOf(RaftEndpoint endpoint) {
        Assert.assertNotNull(endpoint);
        for (int i = 0; i < endpoints.length; i++) {
            if (endpoint.equals(endpoints[i])) {
                return i;
            }
        }
        throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
    }

    public void destroy() {
        stripedExecutor.shutdown();
        scheduledExecutor.shutdown();
        cachedExecutor.shutdown();
    }

    public int size() {
        return endpoints.length;
    }

    /**
     * Split nodes with these indexes from rest of the cluster.
     */
    public void split(int... indexes) {
        assertThat(indexes.length, greaterThan(0));
        assertThat(indexes.length, lessThan(size()));

        int[] firstSplit = new int[size() - indexes.length];
        int[] secondSplit = indexes;

        int ix = 0;
        for (int i = 0; i < size(); i++) {
            if (Arrays.binarySearch(indexes, i) < 0) {
                firstSplit[ix++] = i;
            }
        }

        System.err.println("Creating splits. firstSplit = " + Arrays.toString(firstSplit)
            + ", secondSplit = " + Arrays.toString(secondSplit));
        split(secondSplit, firstSplit);
        split(firstSplit, secondSplit);

    }

    private void split(int[] firstSplit, int[] secondSplit) {
        for (int i : firstSplit) {
            for (int j : secondSplit) {
                integrations[i].removeNode(nodes[j]);
            }
        }
    }

    /**
     * Split nodes having these endpoints from rest of the cluster.
     */
    public void split(RaftEndpoint...endpoints) {
        int[] indexes = new int[endpoints.length];
        for (int i = 0; i < indexes.length; i++) {
            indexes[i] = getIndexOf(endpoints[i]);
        }
        split(indexes);
    }

    public int[] createMinoritySplitIndexes(boolean includingLeader) {
        return createSplitIndexes(includingLeader, minority(size()));
    }

    public int[] createMajoritySplitIndexes(boolean includingLeader) {
        return createSplitIndexes(includingLeader, majority(size()));
    }

    public int[] createSplitIndexes(boolean includingLeader, int splitSize) {
        int leader = getLeaderIndex();

        int[] indexes = new int[splitSize];
        int ix = 0;

        if (includingLeader) {
            indexes[0] = leader;
            ix = 1;
        }

        for (int i = 0; i < size(); i++) {
            if (i == leader) {
                continue;
            }
            if (ix == indexes.length) {
                break;
            }
            indexes[ix++] = i;
        }
        return indexes;
    }

    public void merge() {
        initDiscovery();
    }
}
