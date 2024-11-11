package org.noova.webserver;

import org.noova.tools.Logger;
import org.noova.webserver.header.HttpHeader;
import org.noova.webserver.host.Host;
import org.noova.webserver.host.HostManager;
import org.noova.webserver.session.SessionImpl;
import org.noova.webserver.session.SessionManager;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

// Provided as part of the framework code

public class RequestImpl implements Request {

  private static final Logger log = Logger.getLogger(RequestImpl.class);
  String method;
  String url;
  String protocol;
  InetSocketAddress remoteAddr;
  Map<String,String> headers;
  Map<String,String> queryParams;
  Map<String,String> params;
  byte bodyRaw[];
  org.noova.webserver.Server server;


  private boolean setCookie = false;

  public String cookieId;

  private boolean secure = false;

  public RequestImpl(String methodArg, String urlArg, String protocolArg, Map<String, String> headersArg, Map<String, String> queryParamsArg, Map<String, String> paramsArg, InetSocketAddress remoteAddrArg, byte bodyRawArg[], Server serverArg) {
    method = methodArg;
    url = urlArg;
    remoteAddr = remoteAddrArg;
    protocol = protocolArg;
    headers = headersArg;
    queryParams = queryParamsArg;
    params = paramsArg;
    bodyRaw = bodyRawArg;
    server = serverArg;
  }

  public String requestMethod() {
  	return method;
  }
  public void setParams(Map<String,String> paramsArg) {
    params = paramsArg;
  }

  public void setQueryParams(Map<String,String> queryParamsArg) {
    queryParams = queryParamsArg;
  }

  public int port() {
  	return remoteAddr.getPort();
  }
  public String url() {
  	return url;
  }
  public String protocol() {
  	return protocol;
  }
  public String contentType() {
  	return headers.get("content-type");
  }
  public String ip() {
  	return remoteAddr.getAddress().getHostAddress();
  }
  public String body() {
    return new String(bodyRaw, StandardCharsets.UTF_8);
  }
  public byte[] bodyAsBytes() {
  	return bodyRaw;
  }
  public int contentLength() {
  	return bodyRaw.length;
  }
  public String headers(String name) {
  	return headers.getOrDefault(name.toLowerCase(), null);
  }
  public Set<String> headers() {
  	return headers.keySet();
  }
  public String queryParams(String param) {
  	return queryParams.get(param);
  }
  public Set<String> queryParams() {
  	return queryParams.keySet();
  }
  public String params(String param) {
    return params.get(param);
  }
  public Map<String,String> params() {
    return params;
  }

  public Session session(){

    Host host = HostManager.getHost(headers(HttpHeader.HOST.getHeader()));

    log.info("Host Session: " + host.getHostName());

    SessionManager sessionManager = host.getSessionManager();

    SessionImpl session = (SessionImpl) sessionManager.getSession(this);


    if (session == null || session.isExpired() || session.isInvalidated()) {
      session = (SessionImpl) sessionManager.createSession();
      // tells the router to set the cookie
      setCookie = true;
      cookieId = session.id();
    }

    log.info("Req SessionId: " + session.id());
    return session;
  }

    public boolean isSetCookie() {
    return setCookie;
  }

    public String getCookieId() { return cookieId; }

    public void setSecure(boolean secure) {
        this.secure = secure;
    }

    public boolean isSecure() {
        return secure;
    }

}
