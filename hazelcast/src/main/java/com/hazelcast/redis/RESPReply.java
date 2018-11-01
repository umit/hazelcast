package com.hazelcast.redis;

import com.hazelcast.internal.networking.OutboundFrame;

import java.nio.ByteBuffer;

public final class RESPReply implements OutboundFrame {

    public static final RESPReply OK = new RESPReply("+OK\r\n".getBytes());

    public static final RESPReply NULL = new RESPReply("*-1\r\n".getBytes());

    public static RESPReply ok() {
        return OK;
    }

    public static RESPReply nil() {
        return NULL;
    }

    public static RESPReply integer(int i) {
        String resp = ":" + i + "\r\n";
        return new RESPReply(resp.getBytes());
    }

    public static RESPReply error(String s) {
        String resp = "-ERR " + s + "\r\n";
        return new RESPReply(resp.getBytes());
    }

    public static RESPReply string(String s) {
        String resp = "+" + s + "\r\n";
        return new RESPReply(resp.getBytes());
    }

    public static RESPReply multi(String[] s) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(s.length).append("\r\n");

        for (String str : s) {
            sb.append("$").append(str.getBytes().length).append("\r\n").append(str).append("\r\n");
        }

        return new RESPReply(sb.toString().getBytes());
    }

    private final ByteBuffer message;

    private RESPReply(byte[] bytes) {
        message = ByteBuffer.wrap(bytes);
    }

    public ByteBuffer getMessage() {
        return message;
    }

    @Override
    public boolean isUrgent() {
        return false;
    }

    @Override
    public int getFrameLength() {
        return message.capacity();
    }
}
