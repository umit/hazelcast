package com.hazelcast.raft.impl.util;

/**
 * TODO: Javadoc Pending...
 *
 */
public class Tuple2<X, Y> {

    public final X element1;
    public final Y element2;

    public Tuple2(X element1, Y element2) {
        this.element1 = element1;
        this.element2 = element2;
    }

    @Override
    public String toString() {
        return "Tuple2{" + "element1=" + element1 + ", element2=" + element2 + '}';
    }
}
