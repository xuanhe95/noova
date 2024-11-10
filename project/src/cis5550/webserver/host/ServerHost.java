package cis5550.webserver.host;

import cis5550.webserver.Server;
import cis5550.webserver.session.SessionManager;
import cis5550.webserver.ssl.SSLManager;

/**
 * @author Xuanhe Zhang
 */
public class ServerHost implements Host{

    private static final ServerHost INSTANCE = new ServerHost();

    private final SessionManager sessionManager = new SessionManager();

    private ServerHost(){}

    public static ServerHost getInstance(){
        return INSTANCE;
    }

    @Override
    public SessionManager getSessionManager() {
        return sessionManager;
    }

    @Override
    public String getHostName() {
        return "Server Main Host";
    }

    @Override
    public String getRoot() {
        return cis5550.webserver.Server.staticFiles.root;
    }

    @Override
    public void setRoot(String root) {
        Server.staticFiles.root = root;
    }

    @Override
    public String getKeyStorePath() {
        return SSLManager.CERTIFICATE_PATH;
    }

    @Override
    public void setKeyStorePath(String keyStorePath) {

    }

    @Override
    public String getKeyStoreSecret() {
        return SSLManager.CERTIFICATE_SECRET;
    }

    @Override
    public void setKeyStoreSecret(String keyStoreSecret) {

    }
}
