package cis5550.flame.operation;

import cis5550.kvs.KVS;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Logger;
import cis5550.webserver.Request;
import cis5550.webserver.Response;

import java.io.IOException;
import java.util.Iterator;

public interface Operation {
    static final Logger log = Logger.getLogger(Operation.class);
    String execute(Request req, Response res, OperationContext context);

    public static Iterator<Row> getPartitionRows(KVS kvs, String input, OperationContext ctx) {
        String from = ctx.from();
        String to = ctx.to();
        log.info("from = " + from);
        log.info("to = " + to);
        try {
            return kvs.scan(input, from, to);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    interface PairToPair extends Operation {
    }

    interface RddToPair extends Operation{
    }

    interface PairToRdd extends Operation{
    }

    interface RddToRdd extends Operation{
    }



}
