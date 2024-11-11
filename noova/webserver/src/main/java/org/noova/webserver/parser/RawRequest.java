package org.noova.webserver.parser;


import org.noova.webserver.header.HttpHeader;

import java.util.Map;

class RawRequest {

    boolean valid = true;

    public RawRequest() {
    }

    public RawRequest(String method, String uri, String version, Map<String, String> headers, byte[] body) {
        this.method = method;
        this.uri = uri;
        this.version = version;
        this.headers = headers;
        this.body = body;
    }
    String method;
    String uri;
    String version;
    Map<String, String> headers;
    byte[] body;

    public String getMethod() {
        return method;
    }

    public String getUri() {
        return uri;
    }

    public String getHost(){
        return headers.getOrDefault(HttpHeader.HOST.getHeader(), null) == null ? null : headers.get(HttpHeader.HOST.getHeader()).split(":")[0].strip();
    }
    public String getVersion() {
        return version;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public byte[] getBody() {
        return body;
    }

    public boolean isValid(){
        return valid;
    }

    public void setValid(boolean valid){
        this.valid = valid;
    }



}