package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.raft.RaftConfig;
import com.hazelcast.config.raft.RaftGroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IndeterminateOperationStateException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.HazelcastRaftTestSupport;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.proxy.DefaultRaftReplicateOp;
import com.hazelcast.raft.impl.service.proxy.InvocationTargetLeaveAware;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.version.MemberVersion.UNKNOWN;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftInvocationFailureTest extends HazelcastRaftTestSupport {

    private static final AtomicInteger COMMIT_COUNT = new AtomicInteger();

    private HazelcastInstance[] instances;
    private String groupName = "group";
    private int groupSize = 3;

    @Before
    public void setup() {
        instances = newInstances(groupSize);
        COMMIT_COUNT.set(0);
    }

    @Test(timeout = 60000)
    public void test_invocationFailsOnMemberLeftException() throws ExecutionException, InterruptedException {
        RaftGroupId groupId = getRaftInvocationManager(instances[0]).createRaftGroup(groupName).get();

        HazelcastInstance leader = getLeaderInstance(instances, groupId);

        Future f = new RaftInvocation(getOperationServiceImpl(leader).invocationContext,
                getRaftInvocationManager(leader).getRaftInvocationContext(), groupId,
                new DefaultRaftReplicateOp(groupId, new CustomResponseOp()), 10, 50, 60000).invoke();

        try {
            f.get();
            fail();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof IndeterminateOperationStateException);
        }

        assertTrue(COMMIT_COUNT.get() <= groupSize);
    }

    @Test(timeout = 60000)
    public void test_invocationFailsWithFirstMemberLeftException_when_thereAreRetryableExceptionsAfterwards()
            throws ExecutionException, InterruptedException {
        RaftGroupId groupId = getRaftInvocationManager(instances[0]).createRaftGroup(groupName).get();

        HazelcastInstance leader = getLeaderInstance(instances, groupId);

        Future f = new RaftInvocation(getOperationServiceImpl(leader).invocationContext,
                getRaftInvocationManager(leader).getRaftInvocationContext(), groupId,
                new DefaultRaftReplicateOp(groupId, new IdempotentCustomResponseOp()), 10, 50, 60000).invoke();

        try {
            f.get();
            fail();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof IndeterminateOperationStateException);
        }

        assertTrue(COMMIT_COUNT.get() > groupSize);
    }

    @Override
    protected Config createConfig(int groupSize, int metadataGroupSize) {
        Config config = super.createConfig(groupSize, metadataGroupSize);

        RaftConfig raftConfig = config.getRaftConfig();
        raftConfig.getRaftAlgorithmConfig().setFailOnIndeterminateOperationState(true);
        raftConfig.addGroupConfig(new RaftGroupConfig(groupName, groupSize));

        return config;
    }

    public static class CustomResponseOp extends RaftOp {

        @Override
        public Object run(RaftGroupId groupId, long commitIndex) throws Exception {
            if (COMMIT_COUNT.incrementAndGet() <= 3) {
                MemberImpl member = new MemberImpl(new Address("localhost", 1111), UNKNOWN, false);
                throw new MemberLeftException(member);
            }

            throw new CallerNotMemberException("");
        }

        @Override
        protected String getServiceName() {
            return RaftService.SERVICE_NAME;
        }

        @Override
        public void writeData(ObjectDataOutput out) {
        }

        @Override
        public void readData(ObjectDataInput in) {
        }
    }

    public static class IdempotentCustomResponseOp extends RaftOp implements InvocationTargetLeaveAware {

        @Override
        public Object run(RaftGroupId groupId, long commitIndex) throws Exception {
            if (COMMIT_COUNT.incrementAndGet() <= 3) {
                MemberImpl member = new MemberImpl(new Address("localhost", 1111), UNKNOWN, false);
                throw new MemberLeftException(member);
            }

            throw new CallerNotMemberException("");
        }

        @Override
        public boolean isSafeToRetryOnTargetLeave() {
            return true;
        }

        @Override
        protected String getServiceName() {
            return RaftService.SERVICE_NAME;
        }

        @Override
        public void writeData(ObjectDataOutput out) {
        }

        @Override
        public void readData(ObjectDataInput in) {
        }
    }
}
