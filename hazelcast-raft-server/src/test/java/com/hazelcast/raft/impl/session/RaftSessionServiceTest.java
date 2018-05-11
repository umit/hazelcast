package com.hazelcast.raft.impl.session;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftEndpointImpl;
import com.hazelcast.raft.impl.service.HazelcastRaftTestSupport;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.session.operation.CloseSessionsOp;
import com.hazelcast.raft.impl.session.operation.CreateSessionOp;
import com.hazelcast.raft.impl.session.operation.HeartbeatSessionOp;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static com.hazelcast.raft.impl.RaftUtil.getLeaderEndpoint;
import static com.hazelcast.raft.impl.service.RaftServiceUtil.getRaftNode;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftSessionServiceTest extends HazelcastRaftTestSupport {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private HazelcastInstance[] instances;
    private RaftInvocationManager invocationManager;
    private RaftGroupId groupId;

    @Before
    public void setup() throws ExecutionException, InterruptedException {
        int raftGroupSize = 3;
        Address[] raftAddresses = createAddresses(raftGroupSize);
        instances = newInstances(raftAddresses, raftGroupSize, 0);
        invocationManager = getRaftInvocationService(instances[0]);
        groupId = invocationManager.createRaftGroup("sessions", raftGroupSize).get();
    }

    @Test
    public void testSessionCreate() throws ExecutionException, InterruptedException {
        final SessionResponse response = invocationManager.<SessionResponse>invoke(groupId, new CreateSessionOp()).get();
        System.out.println(response);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftSessionService service = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                    SessionRegistry registry = service.getSessionRegistryOrNull(groupId);
                    assertNotNull(registry);
                    assertNotNull(registry.getSession(response.getSessionId()));
                }
            }
        });
    }

    @Test
    public void testSessionHeartbeat() throws ExecutionException, InterruptedException {
        final SessionResponse response = invocationManager.<SessionResponse>invoke(groupId, new CreateSessionOp()).get();
        final long[] expirationTimes = new long[instances.length];
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (int i = 0; i < instances.length; i++) {
                    RaftSessionService service = getNodeEngineImpl(instances[i]).getService(RaftSessionService.SERVICE_NAME);
                    SessionRegistry registry = service.getSessionRegistryOrNull(groupId);
                    assertNotNull(registry);
                    Session session = registry.getSession(response.getSessionId());
                    assertNotNull(session);
                    expirationTimes[i] = session.expirationTime();
                }
            }
        });

        invocationManager.invoke(groupId, new HeartbeatSessionOp(response.getSessionId())).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (int i = 0; i < instances.length; i++) {
                    RaftSessionService service = getNodeEngineImpl(instances[i]).getService(RaftSessionService.SERVICE_NAME);
                    SessionRegistry registry = service.getSessionRegistryOrNull(groupId);
                    assertNotNull(registry);
                    Session session = registry.getSession(response.getSessionId());
                    assertNotNull(session);
                    assertTrue(session.expirationTime() > expirationTimes[i]);
                }
            }
        });
    }

    @Test
    public void testSessionClose() throws ExecutionException, InterruptedException {
        final SessionResponse response = invocationManager.<SessionResponse>invoke(groupId, new CreateSessionOp()).get();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftSessionService service = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                    SessionRegistry registry = service.getSessionRegistryOrNull(groupId);
                    assertNotNull(registry);
                    assertNotNull(registry.getSession(response.getSessionId()));
                }
            }
        });

        invocationManager.invoke(groupId, new CloseSessionsOp(response.getSessionId())).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftSessionService service = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                    SessionRegistry registry = service.getSessionRegistryOrNull(groupId);
                    assertNotNull(registry);
                    assertNull(registry.getSession(response.getSessionId()));
                }
            }
        });
    }

    @Test
    public void testHeartbeatFailsAfterSessionClose() throws ExecutionException, InterruptedException {
        final SessionResponse response = invocationManager.<SessionResponse>invoke(groupId, new CreateSessionOp()).get();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftSessionService service = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                    SessionRegistry registry = service.getSessionRegistryOrNull(groupId);
                    assertNotNull(registry);
                    assertNotNull(registry.getSession(response.getSessionId()));
                }
            }
        });

        invocationManager.invoke(groupId, new CloseSessionsOp(response.getSessionId())).get();

        exception.expect(SessionExpiredException.class);

        invocationManager.invoke(groupId, new HeartbeatSessionOp(response.getSessionId())).get();
    }

    @Test
    public void testShiftSessionExpirationTimes() throws ExecutionException, InterruptedException {
        final SessionResponse response = invocationManager.<SessionResponse>invoke(groupId, new CreateSessionOp()).get();
        final long[] expirationTimes = new long[instances.length];
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (int i = 0; i < instances.length; i++) {
                    RaftSessionService service = getNodeEngineImpl(instances[i]).getService(RaftSessionService.SERVICE_NAME);
                    SessionRegistry registry = service.getSessionRegistryOrNull(groupId);
                    assertNotNull(registry);
                    Session session = registry.getSession(response.getSessionId());
                    assertNotNull(session);
                    expirationTimes[i] = session.expirationTime();
                }
            }
        });

        RaftEndpointImpl leaderEndpoint = getLeaderEndpoint(getRaftNode(instances[0], groupId));
        final HazelcastInstance leader = factory.getInstance(leaderEndpoint.getAddress());
        leader.getLifecycleService().terminate();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (int i = 0; i < instances.length; i++) {
                    Node node = getNode(instances[i]);
                    if (node == null) {
                        continue;
                    }

                    RaftSessionService service = node.nodeEngine.getService(RaftSessionService.SERVICE_NAME);
                    SessionRegistry registry = service.getSessionRegistryOrNull(groupId);
                    assertNotNull(registry);
                    Session session = registry.getSession(response.getSessionId());
                    assertNotNull(session);
                    assertTrue(session.expirationTime() > expirationTimes[i]);
                }
            }
        });

    }

}
