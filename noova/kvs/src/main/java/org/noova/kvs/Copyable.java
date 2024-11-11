package org.noova.kvs;

public interface Copyable<T> extends Cloneable {
    T clone();
}
