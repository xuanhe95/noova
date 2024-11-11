package org.noova.webserver.header;

public class ContentTypeFactory {
    public static ContentTypeHeader get(String suffix){
        return switch (suffix) {
            case "txt" -> ContentTypeHeader.TEXT_PLAIN;
            case "html" -> ContentTypeHeader.TEXT_HTML;
            case "jpg", "jpeg" -> ContentTypeHeader.IMAGE_JPEG;
            default -> ContentTypeHeader.APPLICATION_OCTET_STREAM;
        };
    }
}
