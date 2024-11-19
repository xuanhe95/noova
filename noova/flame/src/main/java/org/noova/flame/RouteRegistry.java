package org.noova.flame;

import org.noova.flame.operation.*;
import org.noova.kvs.KVSClient;
import org.noova.tools.Logger;
import org.noova.webserver.Request;

import java.io.File;

import static org.noova.webserver.Server.post;

public class RouteRegistry {

    private static final Logger log = Logger.getLogger(RouteRegistry.class);

    private final File myJAR;

    public RouteRegistry(File myJAR) {
        this.myJAR = myJAR;
    }

    private OperationContext getOperationContext(Request req) {
        Coordinator.kvs = new KVSClient(req.queryParams("kvs"));
        OperationContext ctx = new OperationContext(req);
        ctx.jar(myJAR);
        return ctx;
    }

    public void flapMap() {
        post("/rdd/flatMap", (request, response) -> {
            return new FlatMapOperation().execute(request, response, getOperationContext(request));
        });
    }

    public void flapMapParallel() {
        post("/rdd/flatMapParallel", (request, response) -> {
            return new FlatMapParallelOperation().execute(request, response, getOperationContext(request));
        });
    }


    public void mapToPair() {
        post("/rdd/mapToPair", (request, response) -> {
            return new MapToPairOperation().execute(request, response, getOperationContext(request));
        });
    }

    public void foldByKey() {
        post("/rdd/foldByKey", (request, response) -> {
            return new FoldByKeyOperation().execute(request, response, getOperationContext(request));
        });
    }

    public void intersection() {
        post("/rdd/intersection", (request, response) -> {
            return new IntersectionOperation().execute(request, response, getOperationContext(request));
        });
    }

    public void sample() {
        post("/rdd/sample", (request, response) -> {
            return new SampleOperation().execute(request, response, getOperationContext(request));
        });
    }

    public void groupBy() {
        post("/rdd/groupBy", (request, response) -> {
            return new GroupByOperation().execute(request, response, getOperationContext(request));
        });
    }

    public void fromTable() {
        post("/rdd/fromTable", (request, response) -> {
            new FromTableOperation().execute(request, response, getOperationContext(request));
            return "OK";
        });
    }


    // RDD & Pair
    public void flatMapToPair() {
        post("/rdd/flatMapToPair", (request, response) -> {
            log.info("[flat map to pair]");
            new FlatMapToPairOperation().execute(request, response, getOperationContext(request));
            return "OK";
        });
    }


    // HW7

    public void join() {
        post("/rdd/join", (request, response) -> {
            new JoinOperation().execute(request, response, getOperationContext(request));
            return "OK";
        });
    }

    // HW7
    public void fold() {
        post("/rdd/fold", (request, response) -> {
            OperationContext ctx = new OperationContext(request);
            new FoldOperation().execute(request, response, getOperationContext(request));
            return "OK";
        });
    }

    // HW7 EC
    public void filter(){
        post("/rdd/filter", (request, response) -> {
            new FilterOperation().execute(request, response, getOperationContext(request));
            return "OK";
        });
    }

    public void mapPartitions() {
        post("/rdd/mapPartitions", (request, response) -> {
            new MapParitionsOperation().execute(request, response, getOperationContext(request));
            return "OK";
        });
    }

    public void cogroup() {
        post("/rdd/cogroup", (request, response) -> {
            new CogroupOperation().execute(request, response, getOperationContext(request));
            return "OK";
        });
    }

    // HW6 EC helper
    public void union(){
        post("/rdd/union", (request, response) -> {
            new UnionOperation().execute(request, response, getOperationContext(request));
            return "OK";
        });
    }
}


