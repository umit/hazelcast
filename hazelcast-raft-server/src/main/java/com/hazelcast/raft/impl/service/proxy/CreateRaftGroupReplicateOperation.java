package com.hazelcast.raft.impl.service.proxy;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.impl.service.operation.metadata.CreateRaftGroupOperation;
import com.hazelcast.raft.operation.RaftOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.hazelcast.raft.impl.service.RaftService.METADATA_GROUP_ID;

/**
 * TODO: Javadoc Pending...
 *
 */
public class CreateRaftGroupReplicateOperation extends RaftReplicateOperation {

    private String serviceName;
    private String name;
    private int nodeCount;

    public CreateRaftGroupReplicateOperation() {
    }

    public CreateRaftGroupReplicateOperation(String serviceName, String name, int nodeCount) {
        super(METADATA_GROUP_ID);
        this.serviceName = serviceName;
        this.name = name;
        this.nodeCount = nodeCount;
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
        Iterator<RaftEndpoint> iterator = endpoints.iterator();
        while (iterator.hasNext()) {
            RaftEndpoint e = iterator.next();
            if (!service.isActive(e)) {
                iterator.remove();
            }
        }
        if (endpoints.size() < nodeCount) {
            throw new IllegalArgumentException("There are not enough active endpoints to create raft group " + name
                + ". Active endpoints: " + endpoints.size() + ", Requested count: " + nodeCount);
        }

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
        return RaftServiceDataSerializerHook.CREATE_RAFT_GROUP_REPLICATE_OP;
    }
}
