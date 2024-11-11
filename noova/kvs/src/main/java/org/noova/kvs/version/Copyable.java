package org.noova.kvs.version;

public interface Copyable<T> extends Cloneable {
    T clone();
}
