package com.hazelcast.instance;

import com.hazelcast.cluster.Joiner;
import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import java.nio.channels.ServerSocketChannel;

public class StaticMemberNodeContext implements NodeContext {
    private final NodeContext delegate;
    private final String uuid;

    public StaticMemberNodeContext(TestHazelcastInstanceFactory factory, Member member) {
        this(factory, member.getUuid(), member.getAddress());
    }

    public StaticMemberNodeContext(TestHazelcastInstanceFactory factory, String uuid, Address address) {
        this.uuid = uuid;
        delegate = factory.getRegistry().createNodeContext(address);
    }

    @Override
    public NodeExtension createNodeExtension(Node node) {
        return new DefaultNodeExtension(node) {
            @Override
            public String createMemberUuid(Address address) {
                return uuid;
            }
        };
    }

    @Override
    public AddressPicker createAddressPicker(Node node) {
        return delegate.createAddressPicker(node);
    }

    @Override
    public Joiner createJoiner(Node node) {
        return delegate.createJoiner(node);
    }

    @Override
    public ConnectionManager createConnectionManager(Node node, ServerSocketChannel serverSocketChannel) {
        return delegate.createConnectionManager(node, serverSocketChannel);
    }
}
