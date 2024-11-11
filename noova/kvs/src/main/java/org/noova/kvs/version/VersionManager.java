package org.noova.kvs.version;

import org.noova.tools.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Xuanhe Zhang
 *
 * VersionManager is a class that manages the versions of a value.
 * It provides methods to get the latest version, get a specific version, and create a new version.
 */
public class VersionManager<T extends Copyable<T>> {
    private static final Logger log = Logger.getLogger(VersionManager.class);

    // AtomicInteger is used to generate version numbers
    // notice that the current version number is always point to the next version
    private final AtomicInteger VERSION_NUM = new AtomicInteger(0);
    // ConcurrentHashMap is used to store the versions, the string key is the version number
    private final Map<String, Version<T>> VERSION_MAP = new ConcurrentHashMap<>();

//    public VersionManager(){
//        // create a dummy version with version number 0
//        Version<T> version = new Version<>(String.valueOf(VERSION_NUM.incrementAndGet()), null);
//        // put the version into the map
//        VERSION_MAP.put(version.getVersion(), version);
//    }

    // The constructor creates the first version of the value with version number 1
    public VersionManager(T value) {
//        // call the default constructor first, which creates a dummy version with version number 0
//        this();
        // create a new version with version number 1
        Version<T> version = new Version<>(String.valueOf(VERSION_NUM.incrementAndGet()), value);
        // put the version into the map
        VERSION_MAP.put(version.getVersion(), version);
    }

    // create a new version of the value, the new version is a copy of the last version
    public Version<T> getNewVersion() {
        // get the original value
        T originalValue = getLastVersion() == null ? null : getLastVersion().getValue();
        // create a new value by cloning the original value
        T newValue = originalValue == null ? null : originalValue.clone();
        // create a new version with the new value
        Version<T> newVersion = new Version<>(String.valueOf(VERSION_NUM.incrementAndGet()), newValue);
        // put the new version into the map

        log.info("new version: " + newVersion.getVersion());
        VERSION_MAP.put(newVersion.getVersion(), newVersion);

        return newVersion;
    }

    public Version<T> getVersion(String versionNumber) {
        // get the version by version number
        Version<T> version = VERSION_MAP.getOrDefault(versionNumber, null);
        // if the version does not exist, return null
        if(version == null){
            return null;
        }
        log.info("current version: " + version.getVersion());
        // otherwise, return the value of the version
        return version;
    }

    public Version<T> getLastVersion() {
        // since the current version number is always point to the next version, we need to minus 1 to get the last version
        // since we have a dummy version with version 0, it is guaranteed that the last version is always there
        return VERSION_MAP.get(String.valueOf(VERSION_NUM.get()));
    }


    // this method should not being exposed to the outside, it is only used by the put method
    private void getNewVersion(T newValue) {
        // create a new version with the new value
        Version<T> newVersion = new Version<>(String.valueOf(VERSION_NUM.incrementAndGet()), newValue);
        // put the new version into the map
        VERSION_MAP.put(newVersion.getVersion(), newVersion);
    }

    public void putNewVersion(T value) {
        getNewVersion(value);
    }
}
