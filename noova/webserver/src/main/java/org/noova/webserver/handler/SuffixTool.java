package org.noova.webserver.handler;

import org.noova.webserver.header.ContentTypeFactory;
import org.noova.webserver.header.Header;

public class SuffixTool {
    public static String getSuffix(String url){
        if(url == null){
            return "";
        }
        String suffix = url.substring(url.lastIndexOf(".") + 1);
        return suffix;
    }



    public static String getContentHeader(String url){
        String suffix = getSuffix(url);
        Header contentTypeHeader = ContentTypeFactory.get(suffix);
        return contentTypeHeader.getHeader();
    }
}
