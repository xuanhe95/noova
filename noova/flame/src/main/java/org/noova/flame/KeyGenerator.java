package org.noova.flame;

import org.noova.tools.Hasher;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class KeyGenerator {

    static Map<String, AtomicInteger> COUNTER_MAP = new ConcurrentHashMap<>();

    private static String getPrefix(){
        return "job-" + count.getAndIncrement() + "-";
//        return "job-" + count.getAndIncrement() + "-";
    }
    static AtomicInteger count = new AtomicInteger(0);
    public static String get(){
        return Hasher.hash(String.valueOf(UUID.randomUUID().toString()));
    }

    public static String getJob(){
        return getPrefix() + Hasher.hash(String.valueOf(UUID.randomUUID().toString()));
    }

    public static String getJob(String jarName){
        AtomicInteger counter = COUNTER_MAP.getOrDefault(jarName, new AtomicInteger(0));
        COUNTER_MAP.put(jarName, counter);
        return getPrefix() + Hasher.hash(jarName) + "." + counter.getAndIncrement();
    }

    public static String get(String prefix){
        return prefix + "-" + Hasher.hash(String.valueOf(UUID.randomUUID().toString()));
    }


}
