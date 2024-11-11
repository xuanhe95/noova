package org.noova.kvs.replica;

import org.noova.kvs.NodeManager;
import org.noova.tools.Logger;
import org.noova.webserver.Request;
import org.noova.webserver.Response;
import org.noova.webserver.Route;

public class ForwardingRouteDecorator implements Route{
    private final Route originalRoute;
    private final Logger log = Logger.getLogger(ForwardingRouteDecorator.class);

    public ForwardingRouteDecorator(Route originalRoute) {
        this.originalRoute = originalRoute;
    }

    @Override
    public Object handle(Request req, Response res) throws Exception {
        if (!req.url().startsWith(ReplicaManager.REPLICA_PATH)) {
            String replicaUrl = ReplicaManager.getReplicaUri(req);
            log.info("[replica] Forwarding to replica: " + replicaUrl);
            ReplicaManager.forwardReplicas(NodeManager.getLowerReplicas(), replicaUrl, req.bodyAsBytes());
        }

        return originalRoute.handle(req, res);
    }
}
