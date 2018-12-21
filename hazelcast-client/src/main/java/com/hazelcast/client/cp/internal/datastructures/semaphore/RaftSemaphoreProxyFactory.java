package com.hazelcast.client.cp.internal.datastructures.semaphore;

import com.hazelcast.client.impl.clientside.ClientMessageDecoder;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.ClientProxyFactory;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.spi.impl.ClientProxyFactoryWithContext;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.spi.client.RaftGroupTaskFactoryProvider;
import com.hazelcast.nio.Bits;
import com.hazelcast.spi.InternalCompletableFuture;

import static com.hazelcast.client.cp.internal.datastructures.semaphore.RaftSessionAwareSemaphoreProxy.prepareClientMessage;
import static com.hazelcast.client.impl.protocol.util.ParameterUtil.calculateDataSize;
import static com.hazelcast.cp.internal.RaftGroupId.dataSize;
import static com.hazelcast.cp.internal.RaftService.getObjectNameForProxy;
import static com.hazelcast.cp.internal.datastructures.semaphore.client.SemaphoreMessageTaskFactoryProvider.GET_SEMAPHORE_TYPE;
import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;

public class RaftSemaphoreProxyFactory extends ClientProxyFactoryWithContext implements ClientProxyFactory {

    private final HazelcastClientInstanceImpl client;

    public RaftSemaphoreProxyFactory(HazelcastClientInstanceImpl client) {
        this.client = client;
    }

    @Override
    public ClientProxy create(String proxyName, ClientContext context) {
        int dataSize1 = ClientMessage.HEADER_SIZE + calculateDataSize(proxyName);
        ClientMessage msg = ClientMessage.createForEncode(dataSize1);
        msg.setMessageType(RaftGroupTaskFactoryProvider.CREATE_TYPE);
        msg.setRetryable(false);
        msg.setOperationName("");
        msg.set(proxyName);
        msg.updateFrameLength();

        String objectName = getObjectNameForProxy(proxyName);
        ClientInvocationFuture f1 = new ClientInvocation(client, msg, objectName).invoke();

        InternalCompletableFuture<CPGroupId> future1 = new ClientDelegatingFuture<CPGroupId>(f1, client.getSerializationService(),
                new ClientMessageDecoder() {
                    @Override
                    public CPGroupId decodeClientMessage(ClientMessage msg) {
                        return RaftGroupId.readFrom(msg);
                    }
                });

        CPGroupId groupId = future1.join();

        int dataSize2 = ClientMessage.HEADER_SIZE + dataSize(groupId) + calculateDataSize(proxyName) + Bits.LONG_SIZE_IN_BYTES
                + Bits.INT_SIZE_IN_BYTES;
        msg = prepareClientMessage(groupId, proxyName, NO_SESSION_ID, dataSize2, GET_SEMAPHORE_TYPE);
        msg.set(proxyName);
        msg.updateFrameLength();

        ClientInvocationFuture f2 = new ClientInvocation(client, msg, proxyName).invoke();

        InternalCompletableFuture<Boolean> future2 = new ClientDelegatingFuture<Boolean>(f2, client.getSerializationService(),
                new ClientMessageDecoder() {
                    @Override
                    public Boolean decodeClientMessage(ClientMessage msg) {
                        return msg.getBoolean();
                    }
                });

        boolean jdkCompatible = future2.join();

        return jdkCompatible
                ? new RaftSessionlessSemaphoreProxy(context, groupId, proxyName, objectName)
                : new RaftSessionAwareSemaphoreProxy(context, groupId, proxyName, objectName);
    }
}
