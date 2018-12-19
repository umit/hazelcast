package com.hazelcast.cp.internal.raft.impl.state;

import com.hazelcast.core.EndpointIdentifier;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.cp.internal.raft.impl.RaftUtil.newRaftMember;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LeaderStateTest {

    private LeaderState state;
    private Set<EndpointIdentifier> remoteEndpoints;
    private int lastLogIndex;

    @Before
    public void setUp() throws Exception {
        lastLogIndex = 123;
        remoteEndpoints = new HashSet<EndpointIdentifier>(asList(
                newRaftMember(5001),
                newRaftMember(5002),
                newRaftMember(5003),
                newRaftMember(5004)));

        state = new LeaderState(remoteEndpoints, lastLogIndex);
    }

    @Test
    public void test_initialState() {
        for (EndpointIdentifier endpoint : remoteEndpoints) {
            assertEquals(0, state.getMatchIndex(endpoint));
            assertEquals(lastLogIndex + 1, state.getNextIndex(endpoint));
        }

        Collection<Long> matchIndices = state.matchIndices();
        assertEquals(remoteEndpoints.size(), matchIndices.size());
        for (long index : matchIndices) {
            assertEquals(0, index);
        }
    }

    @Test
    public void test_nextIndex() {
        Map<EndpointIdentifier, Integer> indices = new HashMap<EndpointIdentifier, Integer>();
        for (EndpointIdentifier endpoint : remoteEndpoints) {
            int index = 1 + RandomPicker.getInt(100);
            state.setNextIndex(endpoint, index);
            indices.put(endpoint, index);
        }

        for (EndpointIdentifier endpoint : remoteEndpoints) {
            int index = indices.get(endpoint);
            assertEquals(index, state.getNextIndex(endpoint));
        }
    }

    @Test
    public void test_matchIndex() {
        Map<EndpointIdentifier, Long> indices = new HashMap<EndpointIdentifier, Long>();
        for (EndpointIdentifier endpoint : remoteEndpoints) {
            long index = 1 + RandomPicker.getInt(100);
            state.setMatchIndex(endpoint, index);
            indices.put(endpoint, index);
        }

        for (EndpointIdentifier endpoint : remoteEndpoints) {
            long index = indices.get(endpoint);
            assertEquals(index, state.getMatchIndex(endpoint));
        }

        Collection<Long> matchIndices = new HashSet<Long>(state.matchIndices());
        assertEquals(new HashSet<Long>(indices.values()), matchIndices);
    }

}
