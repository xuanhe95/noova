package org.noova.webserver.host;

import org.noova.webserver.session.SessionManager;

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
