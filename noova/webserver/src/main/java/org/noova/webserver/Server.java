package org.noova.webserver;

import org.noova.tools.Logger;
import org.noova.tools.SNIInspector;
import org.noova.webserver.filter.Filter;
import org.noova.webserver.filter.FilterManager;
import org.noova.webserver.host.HostManager;
import org.noova.webserver.http.HttpMethod;
import org.noova.webserver.http.HttpVersion;
import org.noova.webserver.io.NetworkIOStrategy;
import org.noova.webserver.io.SocketIOStrategy;
import org.noova.webserver.pool.FixedThreadPool;
import org.noova.webserver.pool.QueryTask;
import org.noova.webserver.pool.ThreadPool;
import org.noova.webserver.route.RouteManager;
import org.noova.webserver.session.SessionDaemon;
import org.noova.webserver.ssl.SSLManager;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @author Xuanhe Zhang
 */
public class Server {

    private static Server instance = null;
    private static boolean running = false;
    public static final int NUM_WORKERS = 100;
    public static final HttpVersion VERSION = HttpVersion.HTTP_1_1;

    public static final String SERVER_NAME = "MyServer/1.0";
    private static final Logger log = Logger.getLogger(Server.class);
    private static int port = 80;
    private static int securePort = -1;


    public static class staticFiles{
        public static String root;
        public static void location(String p){
            HostManager.location(p);
            log.info("Setting web root to " + p);
            init();
        }
    }


    private static void init(){
        if(instance == null){
            log.info("Initializing default server");
            instance = new Server();
        }
    }


    public void run(){
        log.info("Web server start");

        try(ServerSocket serverSocket = new ServerSocket(port)){
            serverLoop(serverSocket);
        } catch (Exception e) {
            log.error("Error accepting connection");
        }
    }


    private void runSni(){
        log.info("Web server SSL & SNI start");
        ThreadPool threadPool = new FixedThreadPool( 1000);

        SNIInspector sniInspector = new SNIInspector();

        try (ServerSocket serverSocket = new ServerSocket(securePort)){

            while(true){
                Socket socket = serverSocket.accept();
                socket.setTcpNoDelay(true);
                Socket s = SSLManager.getSocket(sniInspector, socket);
                if(s != null){
                    s.setTcpNoDelay(true);
                    NetworkIOStrategy io = new SocketIOStrategy(s, true);
                    threadPool.execute(new QueryTask(io));
                } else{
                    log.error("[server] Error creating SSL socket");
                }
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }

    }


    private void serverLoop(ServerSocket serverSocket){
        ThreadPool threadPool = new FixedThreadPool( 1000);
        while(true){
            try {
                Socket socket = serverSocket.accept();
                socket.setTcpNoDelay(true);
                log.info("Incoming connection from" + socket.getRemoteSocketAddress());
                NetworkIOStrategy io = new SocketIOStrategy(socket, false);
                threadPool.execute(new QueryTask(io));
            } catch (IOException e) {
                log.error("[server] Error accepting connection");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    private static void start(){
        startServer();
        startSSLServer();
    }


    private static void startServer(){
        init();
        SessionDaemon.start();

        if(!running){
            Thread httpThread = new Thread(new Runnable(){
                public void run(){
                    instance.run();
                }
            });
            log.info("[server] HTTP Web server started at port: " + Server.port +  " web root is : " + staticFiles.root);

            httpThread.start();
            running = true;
        }
    }

    private static boolean sslServerRunning = false;


    private static void startSSLServer(){
        init();
        SessionDaemon.start();

        if(Server.securePort == -1){
            log.warn("[server] Secure port not set");
            return;
        }

        if(!sslServerRunning){
            Thread httpsThread = new Thread(new Runnable(){
                public void run(){
                    instance.runSni();
                }
            });
            log.info("[server] HTTPs Web server started at secure port: " + Server.securePort);

            httpsThread.start();

            sslServerRunning = true;
        }
    }

    private static void startDaemon(){
        SessionDaemon.start();
    }


    public static void host(String hostName){
        log.info("[server] Setting host to " + hostName);
        HostManager.addHost(hostName);

    }

    public static void host(String hostName, String keyStorePath, String keyStorePassword){
        if(hostName == null || keyStorePath == null || keyStorePassword == null){
            log.error("[server] Invalid host or keystore path or password");
            throw new RuntimeException("Invalid host or keystore path or password");
        }

        HostManager.addHost(hostName);
        SSLManager.createSSLContext(hostName, keyStorePath, keyStorePassword);
    }


    public static void port(int port){
        log.info("Setting port to " + port);

        if(port == securePort){
            log.error("Port cannot be the same as the secure port");
            throw new RuntimeException("Port cannot be the same as the secure port");
        }

        if(port < 0 || port > 65535){
            log.error("Port number out of range");
            throw new RuntimeException("Port number out of range");
        }

        Server.port = port;
        startServer();
    }

    public static void location(String root){
        HostManager.location(root);
    }


    public static void get(String path, org.noova.webserver.Route route){
        start();
        log.info("GET " + path);
        RouteManager.addRoute(HttpMethod.GET, path, route, HostManager.getHost());
    }

    public static void put(String path, org.noova.webserver.Route route){
        start();
        log.info("PUT " + path);
        RouteManager.addRoute(HttpMethod.PUT, path, route, HostManager.getHost());
    }

    public static void post(String path, Route route){
        start();
        log.info("POST " + path);
        RouteManager.addRoute(HttpMethod.POST, path, route, HostManager.getHost());
    }

    public static void before(Filter filter) {
        FilterManager.before(filter);
    }

    public static void after(Filter filter) {
        FilterManager.after(filter);
    }

    public static void securePort(int securePort){
        log.info("Setting secure port to " + securePort);

        if(Server.port == securePort){
            log.error("Secure port cannot be the same as the normal port");
            throw new RuntimeException("Secure port cannot be the same as the normal port");
        }

        if(securePort < 0 || securePort > 65535){
            log.error("Secure port number out of range");
            throw new RuntimeException("Secure port number out of range");
        }

        Server.securePort = securePort;
        startSSLServer();
    }

}
