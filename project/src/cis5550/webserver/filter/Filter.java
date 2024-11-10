package cis5550.webserver.filter;

import cis5550.webserver.Request;
import cis5550.webserver.Response;

@FunctionalInterface
public interface Filter {

    String filt(Request req, Response rsp);

}
