package com.hazelcast.raft.impl;

import java.util.ArrayList;
import java.util.List;

/**
 * TODO: Javadoc Pending...
 *
 * @author mdogan 30.10.2017
 */
public class RaftLog {

    final List<LogEntry> logs = new ArrayList<LogEntry>();

    public int lastLogIndex() {
        return lastLogEntry().index;
    }

    public int lastLogTerm() {
        return lastLogEntry().term;
    }

    private LogEntry lastLogEntry() {
        return logs.isEmpty() ? new LogEntry() : logs.get(logs.size() - 1);
    }

    public LogEntry getLog(int index) {
        return logs.size() > index ? logs.get(index) : null;
    }

    public void deleteRange(int from, int to) {
        throw new UnsupportedOperationException();
    }

    public void deleteAfter(int index) {
        for (int i = logs.size() - 1; i >= index ; i--) {
            logs.remove(i);
        }
    }

    public void store(LogEntry[] newEntries) {
        for (LogEntry entry : newEntries) {
            logs.add(entry);
        }
    }
}
