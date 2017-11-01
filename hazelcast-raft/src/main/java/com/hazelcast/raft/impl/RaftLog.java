package com.hazelcast.raft.impl;

import java.util.ArrayList;
import java.util.Collections;

/**
 * TODO: Javadoc Pending...
 *
 * !!! INDEX starts from 1 !!!
 */
public class RaftLog {

    private final ArrayList<LogEntry> logs = new ArrayList<LogEntry>();

    public int lastLogIndex() {
        return lastLogEntry().index();
    }

    public int lastLogTerm() {
        return lastLogEntry().term();
    }

    private LogEntry lastLogEntry() {
        return logs.isEmpty() ? new LogEntry() : logs.get(logs.size() - 1);
    }

    public LogEntry getEntry(int index) {
        return logs.size() >= index ? logs.get(index - 1) : null;
    }

    public void deleteEntriesAfter(int index) {
        for (int i = logs.size() - 1; i >= index - 1; i--) {
            logs.remove(i);
        }
    }

    public void storeEntries(LogEntry... newEntries) {
        Collections.addAll(logs, newEntries);
    }
}
