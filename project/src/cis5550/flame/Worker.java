package cis5550.flame;

import java.io.*;

import static cis5550.webserver.Server.*;

import cis5550.tools.Logger;

class Worker extends cis5550.generic.Worker {

    private static final Logger log = Logger.getLogger(Worker.class);

	public static void main(String args[]) {
        if (args.length != 2) {
            System.err.println("Syntax: Worker <port> <coordinatorIP:port>");
            System.exit(1);
        }


        int port = Integer.parseInt(args[0]);
        String server = args[1];
        startPingThread(server, "" + port, port);
        final File myJAR = new File("__worker" + port + "-current.jar");

        port(port);

        post("/useJAR", (request, response) -> {
            FileOutputStream fos = new FileOutputStream(myJAR);
            fos.write(request.bodyAsBytes());
            fos.close();
            return "OK";
        });

        RouteRegistry routes = new RouteRegistry(myJAR);
        routes.flapMap();
        routes.mapToPair();
        routes.foldByKey();

        // HW6 plus
        routes.union();
        routes.intersection();
        routes.sample();
        routes.groupBy();
        // HW7
        routes.fromTable();
        routes.flatMapToPair();
        routes.join();
        routes.fold();
        // HW7 EC
        routes.filter();
        routes.mapPartitions();
        routes.cogroup();


    }

}
