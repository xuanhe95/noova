package org.noova.crawler;

import org.noova.tools.URLParser;

public class ParseLinks {
    static String normalizeURL(String rawUrl, String baseUrl){
        if(rawUrl.contains("#")){
            rawUrl = rawUrl.substring(0, rawUrl.indexOf("#"));
        }

        rawUrl = rawUrl.trim();
        rawUrl = rawUrl.replaceAll("\\\\+$", "");

        if(rawUrl.isEmpty()){
            return null;
        }

        if (rawUrl.matches(".*[<>\"'{}|^\\[\\]]+.*")) { // invalid char in html detected
            return null;
        }
        if(rawUrl.startsWith("..")){
            rawUrl = rawUrl.replace("..", "");
            rawUrl = baseUrl.substring(0, baseUrl.lastIndexOf("/")) + rawUrl;
        }

        try{
            rawUrl = rawUrl.replace(" ", "%20");
            String[] parts = URLParser.parseURL(rawUrl);

            String protocol;
            String host;
            String port;
            String path;

            if (parts[0] == null && parts[1] == null) {
                protocol = URLParser.parseURL(baseUrl)[0] == null ? "http" : URLParser.parseURL(baseUrl)[0];
                host = URLParser.parseURL(baseUrl)[1];
                port = URLParser.parseURL(baseUrl)[2] == null ? "http".equals(protocol) ? "80" : "443" : URLParser.parseURL(baseUrl)[2];
                path = parts[3];
            } else {
                protocol = parts[0] == null ? "http" : parts[0];
                host = parts[1] == null ? URLParser.parseURL(baseUrl)[1] : parts[1];
                port = parts[2] == null ? "http".equals(protocol) ? "80" : "443" : parts[2];
                path = parts[3];
            }
            if (host != null) {
                host = host.replaceAll("[^a-zA-Z0-9.-]", "");
            }
            if(path!=null && !path.startsWith("/")) {
                path = "/"+path;
            }
            if (path != null) {
                path = path.replaceAll("//+", "/");
            }
            return protocol + "://" + host + ":" + port + path;
        } catch(Exception e){
            System.out.println("exception");
            return null;
        }

    }
}
