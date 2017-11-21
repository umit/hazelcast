package com.hazelcast.raft.impl.client;

import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IFunction;
import com.hazelcast.nio.Bits;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLongService;
import com.hazelcast.util.ExceptionUtil;

import static com.hazelcast.client.impl.protocol.util.ParameterUtil.calculateDataSize;
import static com.hazelcast.raft.service.atomiclong.client.AtomicLongMessageTaskFactoryProvider.ADD_AND_GET_TYPE;
import static com.hazelcast.raft.service.atomiclong.client.AtomicLongMessageTaskFactoryProvider.COMPARE_AND_SET_TYPE;
import static com.hazelcast.raft.service.atomiclong.client.AtomicLongMessageTaskFactoryProvider.CREATE_TYPE;
import static com.hazelcast.raft.service.atomiclong.client.AtomicLongMessageTaskFactoryProvider.GET_AND_ADD_TYPE;
import static com.hazelcast.raft.service.atomiclong.client.AtomicLongMessageTaskFactoryProvider.GET_AND_SET_TYPE;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftAtomicLong implements IAtomicLong {

    private static final ClientMessageDecoder LONG_RESPONSE_DECODER = new LongResponseDecoder();
    private static final ClientMessageDecoder BOOLEAN_RESPONSE_DECODER = new BooleanResponseDecoder();

    public static IAtomicLong create(HazelcastInstance instance, String name, int nodeCount) {
        RaftAtomicLong atomicLong = new RaftAtomicLong(instance, name);

        int dataSize = ClientMessage.HEADER_SIZE
                + calculateDataSize(name) + Bits.INT_SIZE_IN_BYTES;
        ClientMessage clientMessage = prepareClientMessage(name, dataSize, CREATE_TYPE);
        clientMessage.set(nodeCount);
        clientMessage.updateFrameLength();

        ICompletableFuture<Object> future = atomicLong.invoke(clientMessage, new ClientMessageDecoder() {
            @Override
            public <T> T decodeClientMessage(ClientMessage clientMessage) {
                return null;
            }
        });
        atomicLong.join(future);

        return atomicLong;
    }

    private final HazelcastClientInstanceImpl client;
    private final String name;

    public RaftAtomicLong(HazelcastInstance instance, String name) {
        if (instance instanceof HazelcastClientProxy) {
            this.client = ((HazelcastClientProxy) instance).client;
        } else if (instance instanceof HazelcastClientInstanceImpl) {
            this.client = (HazelcastClientInstanceImpl) instance;
        } else {
            throw new IllegalArgumentException("Unknown client instance! " + instance);
        }
        this.name = name;
    }

    @Override
    public long addAndGet(long delta) {
        return join(addAndGetAsync(delta));
    }

    @Override
    public boolean compareAndSet(long expect, long update) {
        return join(compareAndSetAsync(expect, update));
    }

    @Override
    public long decrementAndGet() {
        return join(decrementAndGetAsync());
    }

    @Override
    public long get() {
        return join(getAsync());
    }

    @Override
    public long getAndAdd(long delta) {
        return join(getAndAddAsync(delta));
    }

    @Override
    public long getAndSet(long newValue) {
        return join(getAndSetAsync(newValue));
    }

    @Override
    public long incrementAndGet() {
        return join(incrementAndGetAsync());
    }

    @Override
    public long getAndIncrement() {
        return join(getAndIncrementAsync());
    }

    @Override
    public void set(long newValue) {
        join(setAsync(newValue));
    }

    @Override
    public void alter(IFunction<Long, Long> function) {
        join(alterAsync(function));
    }

    @Override
    public long alterAndGet(IFunction<Long, Long> function) {
        return join(alterAndGetAsync(function));
    }

    @Override
    public long getAndAlter(IFunction<Long, Long> function) {
        return join(getAndAlterAsync(function));
    }

    @Override
    public <R> R apply(IFunction<Long, R> function) {
        return join(applyAsync(function));
    }

    @Override
    public ICompletableFuture<Long> addAndGetAsync(long delta) {
        ClientMessage clientMessage = encodeRequest(name, delta, ADD_AND_GET_TYPE);
        return invoke(clientMessage, LONG_RESPONSE_DECODER);
    }

    @Override
    public ICompletableFuture<Boolean> compareAndSetAsync(long expect, long update) {
        ClientMessage clientMessage = encodeRequest(name, expect, update, COMPARE_AND_SET_TYPE);
        return invoke(clientMessage, BOOLEAN_RESPONSE_DECODER);
    }

    @Override
    public ICompletableFuture<Long> decrementAndGetAsync() {
        return addAndGetAsync(-1);
    }

    @Override
    public ICompletableFuture<Long> getAsync() {
        return getAndAddAsync(0);
    }

    @Override
    public ICompletableFuture<Long> getAndAddAsync(long delta) {
        ClientMessage clientMessage = encodeRequest(name, delta, GET_AND_ADD_TYPE);
        return invoke(clientMessage, LONG_RESPONSE_DECODER);
    }

    @Override
    public ICompletableFuture<Long> getAndSetAsync(long newValue) {
        ClientMessage clientMessage = encodeRequest(name, newValue, GET_AND_SET_TYPE);
        return invoke(clientMessage, LONG_RESPONSE_DECODER);
    }

    @Override
    public ICompletableFuture<Long> incrementAndGetAsync() {
        return addAndGetAsync(1);
    }

    @Override
    public ICompletableFuture<Long> getAndIncrementAsync() {
        return getAndAddAsync(1);
    }

    @Override
    public ICompletableFuture<Void> setAsync(long newValue) {
        ICompletableFuture future = getAndSetAsync(newValue);
        return future;
    }

    @Override
    public ICompletableFuture<Void> alterAsync(IFunction<Long, Long> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ICompletableFuture<Long> alterAndGetAsync(IFunction<Long, Long> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ICompletableFuture<Long> getAndAlterAsync(IFunction<Long, Long> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R> ICompletableFuture<R> applyAsync(IFunction<Long, R> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return RaftAtomicLongService.SERVICE_NAME;
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void destroy() {
        throw new UnsupportedOperationException();
    }

    private <T> ICompletableFuture<T> invoke(ClientMessage clientMessage, ClientMessageDecoder decoder) {
        ClientInvocationFuture future = new ClientInvocation(client, clientMessage, getName()).invoke();
        return new ClientDelegatingFuture<T>(future, client.getSerializationService(), decoder);
    }

    private <T> T join(ICompletableFuture<T> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private static ClientMessage encodeRequest(String name, long value, int messageTypeId) {
        int dataSize = ClientMessage.HEADER_SIZE
                + calculateDataSize(name) + Bits.LONG_SIZE_IN_BYTES;
        ClientMessage clientMessage = prepareClientMessage(name, dataSize, messageTypeId);
        clientMessage.set(value);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    private static ClientMessage encodeRequest(String name, long value1, long value2, int messageTypeId) {
        int dataSize = ClientMessage.HEADER_SIZE
                + calculateDataSize(name) + 2 * Bits.LONG_SIZE_IN_BYTES;
        ClientMessage clientMessage = prepareClientMessage(name, dataSize, messageTypeId);
        clientMessage.set(value1);
        clientMessage.set(value2);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    private static ClientMessage prepareClientMessage(String name, int dataSize, int messageTypeId) {
        ClientMessage clientMessage = ClientMessage.createForEncode(dataSize);
        clientMessage.setMessageType(messageTypeId);
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("");
        clientMessage.set(name);
        return clientMessage;
    }

    private static class LongResponseDecoder implements ClientMessageDecoder {
        @Override
        public Long decodeClientMessage(ClientMessage clientMessage) {
            return clientMessage.getLong();
        }
    }

    private static class BooleanResponseDecoder implements ClientMessageDecoder {
        @Override
        public Boolean decodeClientMessage(ClientMessage clientMessage) {
            return clientMessage.getBoolean();
        }
    }
}
