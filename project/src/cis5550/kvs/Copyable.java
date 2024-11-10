package cis5550.kvs;

public interface Copyable<T> extends Cloneable {
    T clone();
}
