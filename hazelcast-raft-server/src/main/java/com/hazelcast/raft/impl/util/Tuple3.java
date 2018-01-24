package com.hazelcast.raft.impl.util;

/**
 * TODO: Javadoc Pending...
 *
 */
public class Tuple3<X, Y, Z> {

    public final X element1;
    public final Y element2;
    public final Z element3;

    public Tuple3(X element1, Y element2, Z element3) {
        this.element1 = element1;
        this.element2 = element2;
        this.element3 = element3;
    }

    @Override
    public String toString() {
        return "Tuple3{" + "element1=" + element1 + ", element2=" + element2 + ", element3=" + element3 + '}';
    }
}
