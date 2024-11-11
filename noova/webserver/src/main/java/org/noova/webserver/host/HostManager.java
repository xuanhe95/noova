package org.noova.webserver.host;

import org.noova.tools.Logger;
import org.noova.webserver.Request;
import org.noova.webserver.Response;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Xuanhe Zhang
 *
 * The host manager is responsible for managing the host and virtual host
 */
public class HostManager {
    private static final Logger log = Logger.getLogger(HostManager.class);
    /*
     * The default host is singleton, which is the host that is used when no virtual host is specified
     */
    private static final Host DEFAULT_HOST = ServerHost.getInstance();
    private static final Map<String, Host> VIRTUAL_HOSTS = new HashMap<>();
    /*
     * The current host is the host that is currently being used
     * It should be updated when a new host is added
     */
    private static Host currentHost;

    public static void addHost(String hostName){
        log.info("Adding host: " + hostName);
        currentHost = new VirtualHost(hostName);
        VIRTUAL_HOSTS.put(hostName, currentHost);
    }

    public static void addHost(String hostName, String keyStorePath, String keyStoreSecret){
        currentHost = new VirtualHost(hostName);
        currentHost.setKeyStorePath(keyStorePath);
        currentHost.setKeyStoreSecret(keyStoreSecret);
        VIRTUAL_HOSTS.put(hostName, currentHost);
    }

    public static Host getHost(String hostName){
        return VIRTUAL_HOSTS.getOrDefault(hostName, DEFAULT_HOST);
    }

    public static Host getHost(){
        if(currentHost == null){
            return DEFAULT_HOST;
        }
        return currentHost;
    }

    public static void setCurrentHost(String hostName){
        currentHost = getHost(hostName);
    }

    public static void setHost(Host host){
        currentHost = host;
    }

    public static void location(String location) {
        if (currentHost == null) {

            DEFAULT_HOST.setRoot(location);
        } else {
            currentHost.setRoot(location);
        }
    }

    public static void removeInvalidAndExpiredSessions(){
        DEFAULT_HOST.getSessionManager().removeInvalidAndExpiredSessions();
        VIRTUAL_HOSTS.values().forEach(host -> host.getSessionManager().removeInvalidAndExpiredSessions());
    }

    public static void handle(Request req, Response res){

    }

}
