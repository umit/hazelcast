/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.client.cp.internal.datastructures.atomicref;

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
import com.hazelcast.spi.InternalCompletableFuture;

import static com.hazelcast.client.impl.protocol.util.ParameterUtil.calculateDataSize;
import static com.hazelcast.cp.internal.RaftService.getObjectNameForProxy;

/**
 * Creates client-side proxies for
 * Raft-based {@link com.hazelcast.core.IAtomicReference}
 */
public class RaftAtomicRefProxyFactory extends ClientProxyFactoryWithContext implements ClientProxyFactory {

    private final HazelcastClientInstanceImpl client;

    public RaftAtomicRefProxyFactory(HazelcastClientInstanceImpl client) {
        this.client = client;
    }

    @Override
    public ClientProxy create(String proxyName, ClientContext context) {
        int dataSize = ClientMessage.HEADER_SIZE + calculateDataSize(proxyName);
        ClientMessage msg = ClientMessage.createForEncode(dataSize);
        msg.setMessageType(RaftGroupTaskFactoryProvider.CREATE_TYPE);
        msg.setRetryable(false);
        msg.setOperationName("");
        msg.set(proxyName);
        msg.updateFrameLength();

        String objectName = getObjectNameForProxy(proxyName);
        ClientInvocationFuture f = new ClientInvocation(client, msg, objectName).invoke();

        InternalCompletableFuture<CPGroupId> future = new ClientDelegatingFuture<CPGroupId>(f, client.getSerializationService(),
                new ClientMessageDecoder() {
                    @Override
                    public CPGroupId decodeClientMessage(ClientMessage msg) {
                        return RaftGroupId.readFrom(msg);
                    }
                });

        CPGroupId groupId = future.join();
        return new RaftAtomicRefProxy(context, groupId, proxyName, objectName);
    }
}
