package org.noova.flame;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Vector;

public class Loader {
  public static void invokeRunMethod(File jarFile, String className, FlameContext arg1, Vector<String> arg2) throws IllegalAccessException, InvocationTargetException, MalformedURLException, ClassNotFoundException, NoSuchMethodException {
    URLClassLoader cl = new URLClassLoader(new URL[] { jarFile.toURI().toURL() }, ClassLoader.getSystemClassLoader());
    Class<?> classToLoad = Class.forName(className, true, cl);
    Method method = classToLoad.getMethod("run", FlameContext.class, String[].class);
    method.invoke(null, new Object[] { arg1, arg2.toArray(new String[] {}) });
  }
}