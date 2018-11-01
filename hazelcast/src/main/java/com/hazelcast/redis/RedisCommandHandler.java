package com.hazelcast.redis;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.function.Consumer;

import static com.hazelcast.redis.RESPReply.error;

public class RedisCommandHandler {

    private static final String REDIS_MAP_NAME = "redis";

    private final NodeEngineImpl nodeEngine;

    public RedisCommandHandler(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    public void execute(Object[] args, Consumer<OutboundFrame> connection) {
        String cmd = new String((byte[]) args[0]);
        if (cmd.equals("SET")) { // map.set
            doSet(args, connection);
        } else if (cmd.equals("GETSET")) { // map.put
            doPut(args, connection);
        } else if (cmd.equals("GET")) { // map.get
            doGet(args, connection);
        } else {
            throw new IllegalArgumentException();
        }
    }

    private void doSet(Object[] args, final Consumer<OutboundFrame> outputHandler) {
        Data key = nodeEngine.toData(args[1]);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        Data value = nodeEngine.toData(args[2]);

        Operation op = getMapOperationProvider(REDIS_MAP_NAME).createSetOperation(REDIS_MAP_NAME, key, value, -1, -1);
        op.setCallerUuid(nodeEngine.getNode().getThisUuid());
        nodeEngine.getOperationService().createInvocationBuilder(MapService.SERVICE_NAME, op, partitionId)
                  .setResultDeserialized(false).invoke().andThen(new ExecutionCallback<Object>() {
            @Override
            public void onResponse(Object response) {
                outputHandler.accept(RESPReply.ok());
            }

            @Override
            public void onFailure(Throwable t) {
                outputHandler.accept(RESPReply.error(t.getMessage()));
            }
        });
    }

    private void doPut(Object[] args, final Consumer<OutboundFrame> outputHandler) {
        Data key = nodeEngine.toData(args[1]);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        Data value = nodeEngine.toData(args[2]);

        Operation op = getMapOperationProvider(REDIS_MAP_NAME).createPutOperation(REDIS_MAP_NAME, key, value, -1, -1);
        op.setCallerUuid(nodeEngine.getNode().getThisUuid());
        nodeEngine.getOperationService().createInvocationBuilder(MapService.SERVICE_NAME, op, partitionId)
                  .setResultDeserialized(false).invoke().andThen(new ExecutionCallback<Object>() {
            @Override
            public void onResponse(Object response) {
                String str = new String(((Data) response).toByteArray());
                outputHandler.accept(RESPReply.string(str));
            }

            @Override
            public void onFailure(Throwable t) {
                outputHandler.accept(error(t.getMessage()));
            }
        });

    }

    private void doGet(Object[] args, final Consumer<OutboundFrame> outputHandler) {
        Data key = nodeEngine.toData(args[1]);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);

        Operation op = getMapOperationProvider(REDIS_MAP_NAME).createGetOperation(REDIS_MAP_NAME, key);
        op.setCallerUuid(nodeEngine.getNode().getThisUuid());
        nodeEngine.getOperationService().createInvocationBuilder(MapService.SERVICE_NAME, op, partitionId)
                  .setResultDeserialized(false).invoke().andThen(new ExecutionCallback<Object>() {
            @Override
            public void onResponse(Object response) {
                String str = new String(((Data) response).toByteArray());
                outputHandler.accept(RESPReply.string(str));
            }

            @Override
            public void onFailure(Throwable t) {
                outputHandler.accept(error(t.getMessage()));
            }
        });
    }

    protected final MapOperationProvider getMapOperationProvider(String mapName) {
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.getMapOperationProvider(mapName);
    }

}
