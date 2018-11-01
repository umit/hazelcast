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

package com.hazelcast.nio.redis;

import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.redis.RESPReply;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.util.function.Supplier;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.internal.networking.HandlerStatus.DIRTY;
import static com.hazelcast.nio.IOUtil.compactOrClear;

@PrivateApi
public class RedisEncoder extends OutboundHandler<Supplier<RESPReply>, ByteBuffer> {

    private RESPReply reply;

    @Override
    public void handlerAdded() {
        initDstBuffer();
    }

    @Override
    public HandlerStatus onWrite() {
        compactOrClear(dst);
        try {
            for (; ; ) {
                if (reply == null) {
                    reply = src.get();
                    if (reply == null) {
                        // everything is processed, so we are done
                        return CLEAN;
                    }
                }

                ByteBuffer message = reply.getMessage();
                if (dst.remaining() >= message.remaining()) {
                    dst.put(message);
                    reply = null;
                } else {
                    int limit = message.limit();
                    message.limit(limit - (message.remaining() - dst.remaining()));
                    dst.put(message);
                    message.limit(limit);

                    // the message didn't get written completely, so we are done.
                    return DIRTY;
                }
            }
        } finally {
            dst.flip();
        }
    }
}
