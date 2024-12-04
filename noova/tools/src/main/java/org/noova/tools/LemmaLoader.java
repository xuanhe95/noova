package org.noova.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class LemmaLoader {
    private static final Logger log = Logger.getLogger(LemmaLoader.class);
    private static final Map<String, String> lemmaMap = new HashMap<>();
    private static final Set<String> dictionary = new HashSet<>();

    static {
        String filePath1 = "/models/lemmatization-en.txt";
        try (
                InputStream is = LemmaLoader.class.getResourceAsStream(filePath1);
                BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
        ) {
            if (is == null) {
                log.warn("Warning: " + filePath1 + " not found in resources");
            } else {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.trim().isEmpty()) {
                        String[] parts = line.split("\\s+");
                        if (parts.length == 2) {
                            lemmaMap.put(parts[1].toLowerCase(), parts[0].toLowerCase());
                        } else {
                            log.warn("Skipping malformed line: " + line);
                        }
                    }
                }
                log.info("Lemmatization map loaded successfully. Total entries: " + lemmaMap.size());
            }
        } catch (IOException e) {
            log.error("Error loading lemmatization map from " + filePath1, e);
        }

        String filePath2 = "/models/words.txt";
        try (
                InputStream is = LemmaLoader.class.getResourceAsStream(filePath2);
                BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
        ) {
            if (is == null) {
                log.warn("Warning: " + filePath2 + " not found in resources");
            } else {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.trim().isEmpty()) {
                        dictionary.add(line.trim().toLowerCase());
                    }
                }
                log.info("Dictionary loaded successfully. Total: " + dictionary.size());
            }
        } catch (IOException e) {
            log.error("Error loading dictionary from " + filePath2, e);
        }
    }

    public static String getLemma(String word) {
        if (word == null) return null;
        String lowerCaseWord = word.toLowerCase();
        if(!dictionary.contains(lowerCaseWord)) {
            return null;
        }
        return lemmaMap.getOrDefault(lowerCaseWord, lowerCaseWord);
    }

    public static boolean getDictionary(String word) {
        return dictionary.contains(word.toLowerCase());
    }


    public static Map<String, String> getLemmaMap() {
        return Collections.unmodifiableMap(lemmaMap);
    }
}