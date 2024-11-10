package cis5550.webserver.session;

import cis5550.webserver.Request;
import cis5550.webserver.cookie.Cookie;
import cis5550.webserver.cookie.CookieImpl;
import cis5550.webserver.cookie.SameSite;
import cis5550.tools.Logger;
import cis5550.webserver.header.HttpHeader;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Xuanhe Zhang
 */
public class SessionManager {
    private static final Logger log = Logger.getLogger(SessionManager.class);

    /*
     * This map should be thread-safe because it is shared among multiple threads.
     */
    private final Map<String, cis5550.webserver.Session> SESSIONS = new ConcurrentHashMap<>();


    public void handle(cis5550.webserver.Request req, cis5550.webserver.Response res){
        cis5550.webserver.RequestImpl reqImpl = (cis5550.webserver.RequestImpl) req;
        if(reqImpl.isSetCookie()){
            cis5550.webserver.Session session = getSession(reqImpl.getCookieId());
                res.header(HttpHeader.SET_COOKIE.getHeader(), getCookie(session, reqImpl.isSecure()).toString());
                log.info("Set-Cookie: " + session.id());
        }
    }

    public static Cookie getCookie(cis5550.webserver.Session session, boolean secure){
        Cookie cookie = new CookieImpl("SessionID", session.id());
        if(secure){
            cookie.setSecure(true);
        }
        cookie.setHttpOnly(true);
        cookie.setSameSite(SameSite.Strict);
        return cookie;
    }

    public cis5550.webserver.Session getSession(Request req){
        String sessionId = getSessionIdFromCookie(req.headers(HttpHeader.COOKIE.getHeader()));
        return getSession(sessionId);
    }


    public static String getSessionValue(cis5550.webserver.Session session){
        return  "SessionID=" +session.id();
    }

    private static String getSessionIdFromCookie(String rawCookie){
        if(rawCookie == null){
            return null;
        }
        String[] cookies = rawCookie.split(";");
        for(String cookie : cookies){
            if(cookie.contains("SessionID")){
                return cookie.split("=")[1];
            }
        }
        return null;
    }

    private cis5550.webserver.Session createSession(String id) {
        cis5550.webserver.Session session = new SessionImpl(id);
        SESSIONS.put(id, session);
        return session;
    }

    public cis5550.webserver.Session createSession() {
        String id = generateSessionId();
        log.info("Creating session: " + id);
        return createSession(id);
    }


    private cis5550.webserver.Session getSession(String id) {
        if(id == null){
            return null;
        }

        SessionImpl session = (SessionImpl) SESSIONS.getOrDefault(id, null);
        if(session != null){
            session.updateLastAccessedTime();
        } else{
            log.warn("Session not found: " + id);
        }
        return session;
    }

    public cis5550.webserver.Session removeSession(String id) {
        return SESSIONS.remove(id);
    }

    public void clear() {
        SESSIONS.clear();
    }

    private static String generateSessionId() {
        return UUID.randomUUID().toString();
    }

    public void removeInvalidAndExpiredSessions() {
        SESSIONS.entrySet().removeIf(entry -> {
            SessionImpl session = (SessionImpl) entry.getValue();
            if(session.isInvalidated() || session.isExpired()){
                log.info("Removing session: " + session.id());
            }
            return session.isInvalidated() || session.isExpired();
        });
    }

}
