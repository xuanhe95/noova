package cis5550.webserver;

import cis5550.webserver.host.HostManager;

import static cis5550.webserver.route.RouteManager.getVirtualHostName;

/**
 * @author Xuanhe Zhang
 *
 * The file location manager is responsible for calculating the location of the file
 */
public class FileLocationHelper {
    public static String getLocation(Request req){
        if(req == null){
            return null;
        }

        String hostName = getVirtualHostName(req);

        return HostManager.getHost(hostName).getRoot() + req.url();
    }
}
