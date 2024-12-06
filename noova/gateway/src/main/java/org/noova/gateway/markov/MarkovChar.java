package org.noova.gateway.markov;

import org.noova.gateway.trie.Trie;

import java.util.*;

public class MarkovChar extends MarkovModel {

    private int prefixLength;
    private Trie trie;
    public MarkovChar(int prefixLength, Trie trie) {
        random = new Random();
        this.prefixLength = prefixLength;
        this.trie = trie;
    }

    public void setRandom(int seed){
        random = new Random(seed);
    }

    public ArrayList<String> getFollows(String key){
        ArrayList<String> follows = new ArrayList<>();

        for (int i = 0; i < text.length() - key.length(); i++) {
            String curString = text.substring(i, i + key.length());
            if (curString.equals(key)) {

                String nextChar = text.substring(i + key.length(), i + key.length() + 1);

                // filter out the nextChar that is not in the trie
                if(trie != null && !trie.containsAfter(curString, nextChar.charAt(0))){
                    continue;
                }
                follows.add(nextChar);
            }
        }
        return follows;
    }

    public String getRandomText(int numChars) {

        if (text.length() < prefixLength) {
            return "";
        }

        StringBuilder builder = new StringBuilder();

        int index = random.nextInt(text.length() - prefixLength + 1);
        String key = text.substring(index, index + prefixLength);
        builder.append(key);


        for (int k = 0; k < numChars - prefixLength; k++) {

            ArrayList<String> follows = getFollows(key);
            if (follows.isEmpty()) {
                break;
            }

            String nextChar = follows.get(random.nextInt(follows.size()));
            builder.append(nextChar);

            key = key.substring(1) + nextChar;
        }

        return builder.toString();
    }


    public Set<String> predictWordsWithPrefix(String prefix){
        Set<String> words = new HashSet<>();
        getRandomTextWithPrefix(prefix, words, 9, 300);
        return words;

    }

    public void getRandomTextWithPrefix(String prefix, Set<String> words, int numChars, int stackLevel) {
        if(stackLevel == 0){
            return;
        }

        if (text.length() < prefixLength) {
            return;
        }

        StringBuilder builder = new StringBuilder(prefix);

        for (int k = 0; k < numChars - prefixLength; k++) {

            ArrayList<String> follows = getFollows(prefix);
            if (follows.isEmpty()) {
                break;
            }

            String nextChar = follows.get(random.nextInt(follows.size()));
            builder.append(nextChar);

            String nextWord = builder.toString();

            prefix = prefix.substring(1) + nextChar;
            if(words.contains(nextWord)){
            }
            else if(trie != null && trie.contains(nextWord)){
                words.add(nextWord);
            }

            getRandomTextWithPrefix(prefix, words, numChars-k, stackLevel-1);
        }

    }



    public String getRandomTextWithPrefix(String prefix, int numChars) {

        if (text.length() < prefixLength) {
            return "";
        }

        StringBuilder builder = new StringBuilder(prefix);

        for (int k = 0; k < numChars - prefixLength; k++) {

            ArrayList<String> follows = getFollows(prefix);
            if (follows.isEmpty()) {
                break;
            }

            String nextChar = follows.get(random.nextInt(follows.size()));
            builder.append(nextChar);

            prefix = prefix.substring(1) + nextChar;
        }

        return builder.toString();

    }

}
