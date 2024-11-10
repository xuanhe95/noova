package cis5550.kvs.version;

/**
 * @author Xuanhe Zhang
 *
 * This class is a version that stores a value and a version number.
 */
public class Version<T extends Copyable<T>> {
    private final String version;
    private T value;

    public Version(String version, T value) {
        this.version = version;
        this.value = value;
    }

    public String getVersion() {
        return version;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

}
