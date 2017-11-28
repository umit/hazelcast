package com.hazelcast.raft.impl.service.proxy;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftOperation;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.service.CreateRaftGroupOperation;
import com.hazelcast.raft.impl.service.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * TODO: Javadoc Pending...
 *
 */
public class CreateRaftGroupReplicatingOperation extends RaftReplicatingOperation {

    private String serviceName;
    private String name;
    private int nodeCount;

    public CreateRaftGroupReplicatingOperation() {
    }

    public CreateRaftGroupReplicatingOperation(String serviceName, String name, int nodeCount) {
        this.serviceName = serviceName;
        this.name = name;
        this.nodeCount = nodeCount;
    }

    @Override
    protected RaftGroupId getRaftGroupId() {
        return RaftService.METADATA_GROUP_ID;
    }

    @Override
    protected RaftOperation getRaftOperation() {
        RaftService service = getService();
        Collection<RaftEndpoint> allEndpoints = service.getAllEndpoints();
        if (nodeCount > allEndpoints.size()) {
            throw new IllegalArgumentException("There are " + allEndpoints.size()
                    + " CP nodes. You cannot create a raft group of " + nodeCount + " nodes.");
        }

        List<RaftEndpoint> endpoints = new ArrayList<RaftEndpoint>(allEndpoints);
        Collections.shuffle(endpoints);
        endpoints = endpoints.subList(0, nodeCount);
        return new CreateRaftGroupOperation(serviceName, name, endpoints);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(serviceName);
        out.writeUTF(name);
        out.writeInt(nodeCount);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        serviceName = in.readUTF();
        name = in.readUTF();
        nodeCount = in.readInt();
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.CREATE_RAFT_GROUP_REPLICATING_OP;
    }
}
