package org.noova.tools;

import java.io.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.net.URL;
import java.net.URLClassLoader;

public class Serializer {
  // cache for deserialized objects
  private static final Map<String, Object> objectCache = new ConcurrentHashMap<>();
  // cache for class loading
  private static final Map<String, Class<?>> classCache = new ConcurrentHashMap<>();
//  private static final ThreadLocal<URLClassLoader> threadLocalClassLoader = ThreadLocal.withInitial(() -> null);

  public static byte[] objectToByteArray(Object o) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(o);
      oos.flush();
      return baos.toByteArray();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public static Object byteArrayToObject(byte[] b, File jarFileToLoadClassesFrom) {
    // get a cache key w/ serialized b and jar file
    String cacheKey = Hasher.hash(b.toString()) + ":" + (jarFileToLoadClassesFrom != null ?
            jarFileToLoadClassesFrom.getAbsolutePath() : "default");
    // get cached object to skip deserialization
    if (objectCache.containsKey(cacheKey)) {
      return objectCache.get(cacheKey);
    }

    Object result = null;

    // get a new URLClassLoader if jar exists, else use the current thread's class loader
    try (URLClassLoader newCL = jarFileToLoadClassesFrom != null
            ? new URLClassLoader(new URL[]{jarFileToLoadClassesFrom.toURI().toURL()},
            Thread.currentThread().getContextClassLoader())
            : null) {
      ByteArrayInputStream bais = new ByteArrayInputStream(b);
      ObjectInputStream ois = new ObjectInputStream(bais) {
        protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
          // cache prev loaded class
          String className = desc.getName();
          if (classCache.containsKey(className)) {
            return classCache.get(className);
          }

          try {
            Class<?> x = Class.forName(className, false, null);
            classCache.put(className, x); // cache
            return x;
          } catch (ClassNotFoundException cnfe) {
//            System.err.println("[Serializer] Class not found in default loader: " + className);
            if (newCL != null) {
              try {
                Class<?> cls = newCL.loadClass(className);
                classCache.put(className, cls); // cache
                return cls;
              } catch (ClassNotFoundException e) {
//                System.err.println("[Serializer] Class not found in newCL: " + className);
                throw e;
              }
            }
            throw cnfe;
          }
        }
      };
      result = ois.readObject();

    } catch (Exception e) {
      e.printStackTrace();
    }

    objectCache.put(cacheKey, result); // cache
    return result;
  }
}
