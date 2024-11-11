package org.noova.webserver.header;

public enum ContentTypeHeader implements Header {
    TEXT_HTML("text/html"),
    TEXT_PLAIN("text/plain"),
    IMAGE_JPEG("image/jpeg"),
    APPLICATION_OCTET_STREAM("application/octet-stream");

    private final String value;

    ContentTypeHeader(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public String getHeader() {
        return value;
    }
}
