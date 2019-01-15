/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.semaphore.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.config.cp.CPSemaphoreConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;

/**
 * Client message task for querying semaphore JDK compatibility
 */
public class GetSemaphoreTypeMessageTask extends AbstractSemaphoreMessageTask {

    private String proxyName;

    GetSemaphoreTypeMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        CPSemaphoreConfig config = nodeEngine.getConfig().getCPSubsystemConfig().findSemaphoreConfig(proxyName);
        boolean jdkCompatible = (config != null && config.isJDKCompatible());
        sendResponse(jdkCompatible);
    }

    @Override
    protected Object decodeClientMessage(ClientMessage clientMessage) {
        super.decodeClientMessage(clientMessage);
        proxyName = clientMessage.getStringUtf8();
        return null;
    }
}
