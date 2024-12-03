package org.noova.gateway.controller;


import org.noova.tools.Logger;
import org.noova.webserver.Request;
import org.noova.webserver.Response;
import org.noova.webserver.Server;

import java.lang.reflect.Method;

/**
 * @author Xuanhe Zhang
 */
public class Controller implements IController {

    private static final Logger log = Logger.getLogger(Controller.class);
    public static void registerRoutes() {
        registerRoutes(SearchController.class);
        registerRoutes(AutocompleteController.class);
        registerRoutes(LocationController.class);
        registerRoutes(ImageController.class);
    }

    public static void registerRoutes(Class clazz){
        for (Method method : clazz.getDeclaredMethods()) {
            log.info("[controller] Checking method: " + method.getName());
            if (method.isAnnotationPresent(Route.class)) {
                Route routeAnnotation = method.getAnnotation(Route.class);
                String path = routeAnnotation.path();
                String httpMethod = routeAnnotation.method();
                log.info("[controller] Registering route: " + httpMethod + " " + path);
                switch (httpMethod.toUpperCase()) {
                    case "GET" -> Server.get(path, (req, res) -> invokeMethod(clazz, method, req, res));
                    case "POST" -> Server.post(path, (req, res) -> invokeMethod(clazz, method, req, res));
                    case "PUT" -> Server.put(path, (req, res) -> invokeMethod(clazz, method, req, res));
                }
            }
        }
    }

    private static Object invokeMethod(Class clazz, Method method, Request req, Response res) throws Exception {
        Object instance;

        if (clazz.getMethod("getInstance") != null) {
            instance = clazz.getMethod("getInstance").invoke(null);
        } else {
            instance = clazz.getDeclaredConstructor().newInstance();
        }

        try {
            method.setAccessible(true);
            return method.invoke(instance, req, res);
        } catch (Exception e) {
            log.error("[controller] Error invoking method: " + method.getName());
            e.printStackTrace();
        }
        return null;
    }


}
