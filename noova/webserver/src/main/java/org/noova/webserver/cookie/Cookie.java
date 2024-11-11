package org.noova.webserver.cookie;

public interface Cookie {
    String getName();
    String getValue();
    void setValue(String value);
    boolean isSecure();
    void setSecure(boolean secure);
    boolean isHttpOnly();
    void setHttpOnly(boolean httpOnly);
    void setSameSite(SameSite sameSite);
}
