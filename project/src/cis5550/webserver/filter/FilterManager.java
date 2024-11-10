package cis5550.webserver.filter;

import cis5550.webserver.DynamicResponse;
import cis5550.tools.Logger;
import cis5550.webserver.Request;
import cis5550.webserver.Response;

import java.util.ArrayList;
import java.util.List;

public class FilterManager {
    private static final Logger log = Logger.getLogger(FilterManager.class);
    private static final List<Filter> beforeFilters = new ArrayList<>();
    private static final List<Filter> afterFilters = new ArrayList<>();

    public static void before(Filter filter) {
        beforeFilters.add(filter);
    }

    public static void after(Filter filter) {
        afterFilters.add(filter);
    }

    public static void executeBeforeFilters(Request req, Response res) {
        for (Filter filter : beforeFilters) {
            filter.filt(req, res);
            if(((DynamicResponse) res).isHalted()){
                log.error("Request halted in before");
                return;
            }
        }
    }

    public static void executeAfterFilters(Request req, Response res) {
        for (Filter filter : afterFilters) {
            filter.filt(req, res);
        }
    }

}
