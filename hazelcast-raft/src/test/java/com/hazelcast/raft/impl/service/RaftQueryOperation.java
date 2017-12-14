package com.hazelcast.raft.impl.service;

import com.hazelcast.raft.operation.RaftOperation;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftQueryOperation extends RaftOperation {

    @Override
    public Object doRun(int commitIndex) {
        RaftDataService service = getService();
        return service.get(commitIndex);
    }

    @Override
    public String getServiceName() {
        return RaftDataService.SERVICE_NAME;
    }

}
