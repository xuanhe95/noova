package cis5550.kvs.replica;

import cis5550.generic.NodeManager;
import cis5550.tools.Logger;
import cis5550.webserver.Request;
import cis5550.webserver.Response;
import cis5550.webserver.Route;

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
