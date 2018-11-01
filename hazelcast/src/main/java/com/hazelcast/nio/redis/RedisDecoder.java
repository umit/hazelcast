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
import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.logging.ILogger;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.spi.annotation.PrivateApi;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.nio.IOUtil.compactOrClear;

@PrivateApi
public class RedisDecoder extends InboundHandler<ByteBuffer, Void> {

    private static final int CAPACITY = (int) MemoryUnit.KILOBYTES.toBytes(1);
    private static final int MAX_CAPACITY = (int) MemoryUnit.MEGABYTES.toBytes(1);

    private final boolean sslEnabled;
    private final RedisEncoder encoder;
    private final TcpIpConnection connection;
    private final ILogger logger;

    private ByteBuffer commandBuffer = ByteBuffer.allocate(CAPACITY);
    private Object[] args;

    public RedisDecoder(TcpIpConnection connection, RedisEncoder encoder) {
        IOService ioService = connection.getConnectionManager().getIoService();
        this.sslEnabled = ioService.getSSLConfig() == null ? false : ioService.getSSLConfig().isEnabled();
        this.encoder = encoder;
        this.connection = connection;
        this.logger = ioService.getLoggingService().getLogger(getClass());
    }

    @Override
    public HandlerStatus onRead() throws IOException {
        src.flip();
        try {
            while (src.hasRemaining()) {
                consumeBytes();
                parse();
            }
            return CLEAN;
        } finally {
            compactOrClear(src);
        }
    }

    private void consumeBytes() throws IOException {
        if (!commandBuffer.hasRemaining()) {
            expandBuffer();
        }
        commandBuffer.put(src);
    }

    private void parse() throws IOException {
        int position = commandBuffer.position();
        commandBuffer.flip();

        boolean complete = false;
        try {
            int numberOfArgs = readNumberOfArgs();
            if (numberOfArgs < 0) {
                 return;
            }

            byte[][] args = new byte[numberOfArgs][];
            for (int i = 0; i < numberOfArgs; i++) {
                if (!commandBuffer.hasRemaining()) {
                    return;
                }
                byte dollar = commandBuffer.get();
                assert dollar == '$';

                int argLen = readIntCrLf();
                if (argLen < 0) {
                    return;
                }
                if (commandBuffer.remaining() < argLen + 2) { // argLen + CrLF
                    return;
                }

                args[i] = new byte[argLen];
                commandBuffer.get(args[i]);
                byte cr = commandBuffer.get();
                assert cr == '\r';
                byte lf = commandBuffer.get();
                assert lf == '\n';
            }
            complete = true;

            for (byte[] arg : args) {
                logger.severe(new String(arg));
            }

        } finally {
            if (complete) {
                commandBuffer.compact();
            } else {
                commandBuffer.flip();
                commandBuffer.limit(commandBuffer.capacity());
                commandBuffer.position(position);
            }
        }
    }

    private int readNumberOfArgs() throws IOException {
        byte asterisk = commandBuffer.get();
        assert asterisk == '*';

        return readIntCrLf();
    }

    private int readIntCrLf() throws IOException {
        int size = 0;
        while (commandBuffer.hasRemaining()) {
            if (commandBuffer.get(commandBuffer.position()) == '\r') {
                // consume
                commandBuffer.get();
                if (commandBuffer.remaining() <= 1) {
                    return -1;
                }
                byte b = commandBuffer.get();
                if ( b != '\n') {
                    throw new IOException("bla ...");
                }
                break;
            }
            size *= 10;
            size += Character.getNumericValue(commandBuffer.get());
        }
        return size;
    }

    private void expandBuffer() throws IOException {
        if (commandBuffer.capacity() == MAX_CAPACITY) {
            throw new IOException("Max command size capacity [" + MAX_CAPACITY + "] has been reached!");
        }

        int capacity = commandBuffer.capacity() << 1;
        if (logger.isFineEnabled()) {
            logger.fine("Expanding buffer capacity to " + capacity);
        }

        ByteBuffer newBuffer = ByteBuffer.allocate(capacity);
        commandBuffer.flip();
        newBuffer.put(commandBuffer);
        commandBuffer = newBuffer;
    }
}
