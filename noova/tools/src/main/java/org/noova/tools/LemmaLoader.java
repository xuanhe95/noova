package org.noova.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class LemmaLoader {
    private static final Logger log = Logger.getLogger(LemmaLoader.class);
    private static final Map<String, String> lemmaMap = new HashMap<>();

    static {
        String filePath = "/models/lemmatization-en.txt";
        try (InputStream is = LemmaLoader.class.getResourceAsStream(filePath)) {
            if (is == null) {
                log.warn("Warning: " + filePath + " not found in resources");
            } else {
                BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.trim().isEmpty()) {
                        String[] parts = line.split("\\s+");
                        if (parts.length == 2) {
                            lemmaMap.put(parts[0].toLowerCase(), parts[1].toLowerCase());
                        } else {
                            log.warn("Skipping malformed line: " + line);
                        }
                    }
                }
                log.info("Lemmatization map loaded successfully. Total entries: " + lemmaMap.size());
            }
        } catch (IOException e) {
            log.error("Error loading lemmatization map from " + filePath, e);
        }
    }

    public static String getLemma(String word) {
        if (word == null) return null;
        return lemmaMap.get(word.toLowerCase());
    }

    public static Map<String, String> getLemmaMap() {
        return Collections.unmodifiableMap(lemmaMap);
    }
}