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

    public LogEntry getEntry(int entryIndex) {
        return logs.size() >= entryIndex ? logs.get(toArrayIndex(entryIndex)) : null;
    }

    public void deleteEntriesAfter(int entryIndex) {
        for (int i = logs.size() - 1; i >= entryIndex - 1; i--) {
            logs.remove(i);
        }
    }

    public void storeEntries(LogEntry... newEntries) {
        Collections.addAll(logs, newEntries);
    }

    // both inclusive
    public LogEntry[] getEntriesBetween(int fromEntryIndex, int toEntryIndex) {
        if (logs.size() <= fromEntryIndex) {
            return new LogEntry[0];
        }
        return logs.subList(toArrayIndex(fromEntryIndex), toArrayIndex(toEntryIndex + 1)).toArray(new LogEntry[0]);
    }

    private int toArrayIndex(int entryIndex) {
        return entryIndex - 1;
    }
}
