package cis5550.kvs.version;

public interface Copyable<T> extends Cloneable {
    T clone();
}
