package org.noova.webserver;

@FunctionalInterface
public interface Route {
  Object handle(Request request, Response response) throws Exception;
}
