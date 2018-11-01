package com.hazelcast.redis;

import com.hazelcast.nio.Packet;

final class RESPReply {

    public static final byte[] OK = "+OK\r\n".getBytes();

    public static final byte[] NULL = "*-1\r\n".getBytes();

    private RESPReply() {
    }

    public static Packet ok() {
        return new Packet("+OK\r\n".getBytes());
    }

    public static Packet nil() {
        return new Packet("*-1\r\n".getBytes());
    }

    public static Packet integer(int i) {
        String resp = ":" + i + "\r\n";
        return new Packet(resp.getBytes());
    }

    public static Packet error(String s) {
        String resp = "-ERR " + s + "\r\n";
        return new Packet(resp.getBytes());
    }

    public static Packet string(String s) {
        String resp = "+" + s + "\r\n";
        return new Packet(resp.getBytes());
    }

    public static Packet multi(String[] s) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(s.length).append("\r\n");

        for (String str : s) {
            sb.append("$").append(str.getBytes().length).append("\r\n").append(str).append("\r\n");
        }

        return new Packet(sb.toString().getBytes());
    }

}
