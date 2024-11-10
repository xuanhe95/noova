package cis5550.webserver;

import cis5550.tools.Logger;
import cis5550.tools.SNIInspector;
import cis5550.webserver.filter.Filter;
import cis5550.webserver.filter.FilterManager;
import cis5550.webserver.host.HostManager;
import cis5550.webserver.http.HttpMethod;
import cis5550.webserver.http.HttpVersion;
import cis5550.webserver.io.NetworkIOStrategy;
import cis5550.webserver.io.SocketIOStrategy;
import cis5550.webserver.pool.FixedThreadPool;
import cis5550.webserver.pool.QueryTask;
import cis5550.webserver.pool.ThreadPool;
import cis5550.webserver.route.RouteManager;
import cis5550.webserver.session.SessionDaemon;
import cis5550.webserver.ssl.SSLManager;

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


    public static void get(String path, cis5550.webserver.Route route){
        start();
        log.info("GET " + path);
        RouteManager.addRoute(HttpMethod.GET, path, route, HostManager.getHost());
    }

    public static void put(String path, cis5550.webserver.Route route){
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
