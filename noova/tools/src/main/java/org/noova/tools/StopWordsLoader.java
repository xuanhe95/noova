package org.noova.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class StopWordsLoader {
    private static final Logger log = Logger.getLogger(StopWordsLoader.class);
    private static final Set<String> stopWords = new HashSet<>();

    static {
//        String filePath = "/models/stopwords-en.txt";
//        try (InputStream is = StopWordsLoader.class.getResourceAsStream(filePath)) {
//            if (is == null) {
//                log.warn("Warning: " + filePath + " not found in resources");
//            } else {
//                BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
//                String line;
//                while ((line = reader.readLine()) != null) {
//                    if (!line.trim().isEmpty()) {
//                        stopWords.add(line.trim().toLowerCase());
//                    }
//                }
//                log.info("Stop words loaded successfully. Total: " + stopWords.size());
//            }
//        } catch (IOException e) {
//            log.error("Error loading stop words from " + filePath, e);
//        }

        Collections.addAll(stopWords,
                "a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
                "has", "he", "in", "is", "it", "its", "of", "on", "that", "the",
                "to", "was", "were", "will", "with",
                "the", "this", "but", "they", "have", "had", "what",
                "when", "where", "who", "which", "why", "how",
                "all", "any", "both", "each", "few", "more", "most", "other", "some", "such",
                "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very",
                "can", "did", "does", "doing", "done", "could", "should", "would", "must",
                "my", "me", "your", "you", "his", "her", "their", "our",
                "been", "before", "after", "above", "below", "up", "down", "in", "out", "on", "off",
                "over", "under", "again", "further", "then", "once",
                "i", "im", "ive", "id", "ill", "youre", "youve", "youll", "youve",
                "hes", "shes", "its", "were", "theyre", "weve", "theyve",
                "cannot", "couldnt", "didnt", "doesnt", "dont", "hadnt", "hasnt", "havent",
                "isnt", "might", "mightnt", "mustnt", "neednt", "shant", "shouldnt", "wasnt",
                "wouldnt", "shall"
        );
        log.info("Stop words initialized successfully. Total: " + stopWords.size());
    }

    public static boolean isStopWord(String word) {
        return word != null && stopWords.contains(word.toLowerCase());
    }

    public static Set<String> getStopWords() {
        return new HashSet<>(stopWords);
    }
}

