package cis5550.webserver.host;

import cis5550.webserver.session.SessionManager;

public interface Host {
    String getHostName();

    String getRoot();

    void setRoot(String root);

    String getKeyStorePath();

    void setKeyStorePath(String keyStorePath);

    String getKeyStoreSecret();

    void setKeyStoreSecret(String keyStoreSecret);

    SessionManager getSessionManager();

}
