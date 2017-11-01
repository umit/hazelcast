package com.hazelcast.raft.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class LogEntry implements DataSerializable {
    private int term;
    private int index;

    private Object data;

    public LogEntry() {
    }

    public LogEntry(int term, int index, Object data) {
        this.term = term;
        this.index = index;
        this.data = data;
    }

    public int index() {
        return index;
    }

    public int term() {
        return term;
    }

    public Object data() {
        return data;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(term);
        out.writeInt(index);
        out.writeObject(data);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        term = in.readInt();
        index = in.readInt();
        data = in.readObject();
    }

    @Override
    public String toString() {
        return "LogEntry{" + "term=" + term + ", index=" + index + ", data=" + data + '}';
    }
}
