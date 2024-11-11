package org.noova.webserver.ssl;

import org.noova.tools.Logger;
import org.noova.tools.SNIInspector;
import org.noova.webserver.host.Host;
import org.noova.webserver.host.HostManager;

import javax.net.ServerSocketFactory;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Xuanhe Zhang
 *
 * The SSL manager is responsible for managing the SSL context
 */
public class SSLManager {

    private static final Logger log = Logger.getLogger(SSLManager.class);
    public static final String CERTIFICATE_PATH = "keystore.jks";
    public static final String CERTIFICATE_SECRET = "secret";

    private static SSLContext defaultSSLContext;

    static{
        defaultSSLContext = createSSLContext(null, CERTIFICATE_PATH, CERTIFICATE_SECRET);
    }

    private static final Map<Host, SSLContext> SSL_CONTEXTS = new HashMap<>();

    public static ServerSocket getServerSocket(int securePortNo) {
        try {
            SSLContext sslContext = createSSLContext(null, CERTIFICATE_PATH, CERTIFICATE_SECRET);

            ServerSocketFactory factory = sslContext.getServerSocketFactory();
            ServerSocket serverSocketTLS = factory.createServerSocket(securePortNo);

            return serverSocketTLS;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static Socket getSocket(SNIInspector inspector, Socket socket) {

        try {
            inspector.parseConnection(socket);

            SNIHostName sniHostName = inspector.getHostName();
            SSLContext sslContext = getSSLContext(sniHostName);

            SSLSocketFactory factory = sslContext.getSocketFactory();
            return factory.createSocket(socket, inspector.getInputStream(), true);
        } catch (Exception e) {
            log.error("Error creating SSL socket", e);
        }
        return null;
    }

    public static SSLContext getSSLContext(SNIHostName hostName){
        if(hostName == null){
            return defaultSSLContext;
        }
        Host host = HostManager.getHost(hostName.getAsciiName());
        return SSL_CONTEXTS.getOrDefault(host, defaultSSLContext);
    }

    public static SSLContext getSSLContext(String hostName) {
        if(hostName == null){
            return defaultSSLContext;
        }
        return SSL_CONTEXTS.getOrDefault(HostManager.getHost(hostName), defaultSSLContext);
    }

    public static SSLContext createSSLContext(String hostName, String keyStorePath, String secret) {
        try {
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(new FileInputStream(keyStorePath), secret.toCharArray());
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
            keyManagerFactory.init(keyStore, secret.toCharArray());
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagerFactory.getKeyManagers(), null, null);

            if(hostName == null){
                defaultSSLContext = sslContext;
            } else{
                SSL_CONTEXTS.put(HostManager.getHost(hostName), sslContext);
            }

            return sslContext;
        } catch (Exception e) {
            log.error("Error creating SSL context", e);
            throw new RuntimeException(e);
        }
    }
}
