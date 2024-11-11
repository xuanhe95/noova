package org.noova.kvs.version;

public interface Versionable<T> {
    String getVersion();

    T getValue();

}
