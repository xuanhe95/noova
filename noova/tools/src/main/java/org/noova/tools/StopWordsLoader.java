package org.noova.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

public class StopWordsLoader {
    private static final Logger log = Logger.getLogger(StopWordsLoader.class);
    private static final Set<String> stopWords = new HashSet<>();

    static {
        String filePath = "/models/stopwords-en.txt";
        try (InputStream is = StopWordsLoader.class.getResourceAsStream(filePath)) {
            if (is == null) {
                log.warn("Warning: " + filePath + " not found in resources");
            } else {
                BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.trim().isEmpty()) {
                        stopWords.add(line.trim().toLowerCase());
                    }
                }
                log.info("Stop words loaded successfully. Total: " + stopWords.size());
            }
        } catch (IOException e) {
            log.error("Error loading stop words from " + filePath, e);
        }
    }

    public static boolean isStopWord(String word) {
        return word != null && stopWords.contains(word.toLowerCase());
    }

    public static Set<String> getStopWords() {
        return new HashSet<>(stopWords);
    }
}

