package cis5550.kvs.version;

public interface Versionable<T> {
    String getVersion();

    T getValue();

}
