package org.noova.webserver.filter;

import org.noova.webserver.Request;
import org.noova.webserver.Response;

@FunctionalInterface
public interface Filter {

    String filt(Request req, Response rsp);

}
