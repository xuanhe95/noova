package org.noova.webserver;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

import static org.noova.webserver.Server.*;
class HelloWorldApp {
    public static void main(String args[]) {
        port(8080);
        securePort(443);

        Server.staticFiles.location("static");

//        get("/hello/:name", (req,res) ->
//        { return "Hello "+req.params("name"); } );

        get("/", (req, res) -> {
            res.type("text/html");
            return loadIndexHtml();
        });

        get("/static/:filename", (req, res) -> {
            // Get the file name from the route parameter
            String filename = req.params("filename");

            // If there are subdirectories, handle those by appending the filename
            return loadStaticFile("static/" + filename, res);
        });

    }

    private static Object loadStaticFile(String filePath, Response res) {
        try (InputStream inputStream = HelloWorldApp.class.getClassLoader().getResourceAsStream(filePath)) {
            if (inputStream == null) {
                res.status(404,"not found!");
                return "File not found";
            }

            // Determine and set the MIME type
            if (filePath.endsWith(".png")) {
                res.type("image/png");
            } else if (filePath.endsWith(".ico")) {
                res.type("image/x-icon");
            } else if (filePath.endsWith(".html")) {
                res.type("text/html");
            } else {
                res.type(Files.probeContentType(Paths.get(filePath)));
            }

            return inputStream.readAllBytes();
        } catch (IOException e) {
            res.status(500,"internal server error");
            return "Error loading file";
        }
    }

    private static String loadIndexHtml() {
        try {
            // Load index.html as a resource from the classpath
            InputStream inputStream = HelloWorldApp.class.getClassLoader().getResourceAsStream("static/index.html");

            if (inputStream == null) {
                throw new IOException("File not found in classpath: static/index.html");
            }

            return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
            return "<html><body><h1>Error loading page</h1></body></html>";
        }
    }

    private static List<String> listStaticFiles() {
        List<String> fileList = new ArrayList<>();
        try {
            // Get URL to the static directory in the classpath
            URL staticDir = HelloWorldApp.class.getClassLoader().getResource("static");
            if (staticDir == null) {
                System.out.println("Static directory not found in classpath.");
                return fileList;
            }

            // Check if running from a JAR
            if (staticDir.getProtocol().equals("jar")) {
                String jarPath = staticDir.getPath().substring(5, staticDir.getPath().indexOf("!")); // Strip "jar:" prefix and after "!"
                try (JarFile jar = new JarFile(URLDecoder.decode(jarPath, StandardCharsets.UTF_8.name()))) {
                    Enumeration<JarEntry> entries = jar.entries();
                    while (entries.hasMoreElements()) {
                        String name = entries.nextElement().getName();
                        if (name.startsWith("static/") && !name.endsWith("/")) { // Only include files in static/
                            fileList.add(name.substring("static/".length())); // Make path relative to "static"
                        }
                    }
                }
            } else {
                // If not running from a JAR, assume it's on the file system
                try (var paths = Files.walk(Paths.get(staticDir.toURI()))) {
                    paths.filter(Files::isRegularFile)
                            .map(path -> Paths.get("static").relativize(path)) // Make path relative to "static"
                            .forEach(filePath -> fileList.add(filePath.toString()));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return fileList;
    }
}

