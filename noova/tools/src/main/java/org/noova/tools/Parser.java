package org.noova.tools;

import org.checkerframework.checker.units.qual.K;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Parser {

    public static List<String> getLammelizedWords(String text) {
        // [Process Helper] Get words from text
        // [Process Helper] Lammelized words
        // [Process Helper] Remove non-english words

        if(text==null || text.isEmpty()){
            return new ArrayList<>();
        }

        String[] words = text.split("-+");

        List<String> wordList = new ArrayList<>();

        for(int i = 0; i < words.length; i++){
            System.out.println("word: " + words[i]);
            words[i] = processWord(words[i]);
            words[i] = LemmaLoader.getLemma(words[i]);
            if(words[i] != null && !StopWordsLoader.isStopWord(words[i])){
                wordList.add(words[i]);
            }

        }
        return wordList;
    }


    public static String[] imagesToHtml(String images){
        String delimiter = PropertyLoader.getProperty("delimiter.default");
        String[] imageArray = images.split(delimiter);
        String[] htmlArray = new String[imageArray.length];
        for(int i = 0; i < imageArray.length; i++){
            System.out.println("image: " + imageArray[i]);
            htmlArray[i] = "<img src=\"" + imageArray[i] + "\" />";
        }
        return htmlArray;
    }

    public static String processWord (String rawText){
        // [Process Helper] remove non-eng word + normalize space

        if(rawText==null || rawText.isEmpty()) return rawText;

        // preserve unicode
//        return rawText.replaceAll("[^\\p{Print}]", " ") // non printable rm
//                .replaceAll("\\s+", " ")             // Normalize spaces
//                .trim();

        // preserve ascii
        return rawText.replaceAll("[^\\p{ASCII}]", " ") // non ascii remove
                .replaceAll("\\s+", " ")             // Normalize spaces
                .trim();
    }

    public static String processSingleWord(String rawWord){
        if(rawWord==null || rawWord.isEmpty()) return rawWord;
        return rawWord.replaceAll("[^\\p{ASCII}]", "") // non ascii remove
                .trim();
    }

    public static String removeAfterFirstPunctuation(String input) {

        String punctuation = ".,!?:;\"'(){}[]-";

        int firstPunctuationIndex = -1;

        for (int i = 0; i < input.length(); i++) {
            if (punctuation.indexOf(input.charAt(i)) != -1) {
                firstPunctuationIndex = i; // should be more memory friendly vs split
                break;
            }
        }

        if (firstPunctuationIndex == -1) {
            return input;
        }

        return input.substring(0, firstPunctuationIndex);
    }

    public static String extractNumber(String input) {

        Pattern pattern = Pattern.compile("\\d+");
        Matcher matcher = pattern.matcher(input);
        if (matcher.find()) {
            return matcher.group();
        }
        return null;
    }

    public static <K extends Comparable<? super K>, V> Map<K, V> paginateResults(Map<K, V> data, int limit, int offset) {
        // Sort the entries by key (natural ordering)
        List<Map.Entry<K, V>> sortedEntries = data.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey()) // Ensures keys are comparable
                .toList();

        // Calculate pagination indices
        int fromIndex = Math.min(offset * limit, sortedEntries.size());
        int toIndex = Math.min(fromIndex + limit, sortedEntries.size());

        // Extract paginated entries
        List<Map.Entry<K, V>> paginatedEntries = sortedEntries.subList(fromIndex, toIndex);

        // Convert back to Map
        return paginatedEntries.stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }


    public static void main(String[] args) {
        String testString = "asd123.jpg";
        String result = removeAfterFirstPunctuation(testString);

        System.out.println("Original: " + testString);
        System.out.println("Processed: " + result);
    }

}
