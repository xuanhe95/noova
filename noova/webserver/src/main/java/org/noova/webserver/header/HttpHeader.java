package org.noova.webserver.header;

public enum HttpHeader {
    IF_MODIFIED_SINCE("If-Modified-Since"),

    CONTENT_TYPE("Content-Type"),

    CONTENT_LENGTH("Content-Length"),

    SERVER("Server"),

    RANGE("Range"),

    HOST("Host"),

    LAST_MODIFIED("Last-Modified"),

    CONTENT_RANGE("Content-Range"),

    LOCATION("Location"),

    COOKIE("Cookie"),

    SET_COOKIE("Set-Cookie");

    private final String header;

    HttpHeader(String header) {
        this.header = header;
    }

    public String getHeader() {
        return header.toLowerCase();
    }

}
