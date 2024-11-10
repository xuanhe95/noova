package cis5550.webserver.host;


import cis5550.webserver.session.SessionManager;

/**
 * @author Xuanhe Zhang
 */
public class VirtualHost implements Host{
    private final String hostName;
    private String root;
    private String keyStorePath;
    private String keyStoreSecret;

    private final SessionManager sessionManager = new SessionManager();

    public VirtualHost(String hostName){
        this.hostName = hostName;
    }

    @Override
    public SessionManager getSessionManager() {
        return sessionManager;
    }

    @Override
    public String getHostName() {
        return hostName;
    }

    @Override
    public String getRoot() {
        return root;
    }

    @Override
    public void setRoot(String root) {
        this.root = root;
    }

    @Override
    public String getKeyStorePath() {
        return keyStorePath;
    }

    public void setKeyStorePath(String keyStorePath) {
        this.keyStorePath = keyStorePath;
    }

    public String getKeyStoreSecret() {
        return keyStoreSecret;
    }

    public void setKeyStoreSecret(String keyStoreSecret) {
        this.keyStoreSecret = keyStoreSecret;
    }


}
