package org.noova.tools;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;

import java.io.InputStream;
import java.util.Map;

public class FileParser {

    private static final AutoDetectParser parser = new AutoDetectParser();
    private static final Map<String, String> ICON_MAP = Map.of(
            "application/pdf", "https://cdn-icons-png.flaticon.com/512/4208/4208479.png",
            "application/vnd.ms-excel", "https://download.logo.wine/logo/Microsoft_Excel/Microsoft_Excel-Logo.wine.png",
            "application/msword", "https://upload.wikimedia.org/wikipedia/commons/thumb/f/fd/Microsoft_Office_Word_%282019%E2%80%93present%29.svg/2203px-Microsoft_Office_Word_%282019%E2%80%93present%29.svg.png",
            "application/zip", "https://uxwing.com/wp-content/themes/uxwing/download/file-and-folder-type/zip-icon.png",
            "application/vnd.ms-powerpoint", "https://cdn.pixabay.com/photo/2021/01/30/12/18/powerpoint-5963677_960_720.png"
    );

    private static final String DEFAULT_ICON_URL = "https://cdn-icons-png.flaticon.com/512/1808/1808512.png";

    public static String getIconUrl(String contentType) {
        if (contentType == null || contentType.isEmpty()) {
            return DEFAULT_ICON_URL;
        }
        return ICON_MAP.getOrDefault(contentType, DEFAULT_ICON_URL);
    }

    public static String getTitle(Metadata metadata, String fallbackUrl, String contentType) {
        String title = metadata.get("title");
        if (title != null && !title.isEmpty()) {
            return title;
        } else {
            title = fallbackUrl.substring(fallbackUrl.lastIndexOf('/') + 1).replaceAll("\\.\\w+$", "");
        }

        String domain = "";
        try {
            java.net.URL url = new java.net.URL(fallbackUrl);
            domain = url.getHost().replace("www.", "");
        } catch (Exception e) {
            domain = "unknown domain";
        }


        String fileType = getFileTypeKeyword(contentType);
        if (fileType != null) {
            title += " " + fileType;
        }

        title += " from " + domain;

        return title;
    }

    private static String getFileTypeKeyword(String contentType) {
        if (contentType == null || contentType.isEmpty()) {
            return null;
        }
        if (contentType.contains("pdf")) {
            return "PDF";
        } else if (contentType.contains("word") || contentType.contains("msword")) {
            return "DOC";
        } else if (contentType.contains("excel")) {
            return "XLSX";
        } else if (contentType.contains("powerpoint")) {
            return "PPT";
        } else if (contentType.contains("zip")) {
            return "ZIP";
        } else if (contentType.contains("text/plain")) {
            return "TXT";
        }else if (contentType.contains("xml")) {
            return "XML";
        }else if (contentType.contains("json")) {
            return "JSON";
        }else if (contentType.contains("csv")) {
            return "CSV";
        }else if (contentType.contains("rtf")) {
            return "RTF";
        }

        return null; // Return null if the type is not recognized
    }

    public static ParsedFile extractTextAndMetadata(InputStream inputStream, String contentType) {
        try {
            BodyContentHandler handler = new BodyContentHandler(-1); // No size limit
            Metadata metadata = new Metadata();
            metadata.set(Metadata.CONTENT_TYPE, contentType);
            parser.parse(inputStream, handler, metadata);

            ParsedFile parsedFile = new ParsedFile(handler.toString(), metadata);
            return parsedFile;
        } catch (Exception e) {
            Logger.getLogger(FileParser.class).error("Error parsing file content", e);
            return new ParsedFile("", new Metadata());
        }
    }

    public static class ParsedFile {
        private final String text;
        private final Metadata metadata;

        public ParsedFile(String text, Metadata metadata) {
            this.text = text;
            this.metadata = metadata;
        }

        public String getText() {
            return text;
        }

        public Metadata getMetadata() {
            return metadata;
        }
    }
}
