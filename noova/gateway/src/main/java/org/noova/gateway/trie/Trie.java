package org.noova.gateway.trie;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Trie implements Serializable {
    private static class TrieNode implements Serializable {
        @JsonProperty
        private final Map<Character, TrieNode> children = new HashMap<>();
        @JsonProperty
        private boolean isWord;
        @JsonProperty
        private String word;
        @JsonProperty
        private Object value;

    }
    @JsonProperty
    private final TrieNode TIRE_ROOT;

    public Trie() {
        TIRE_ROOT = new TrieNode();
    }



    public void insert(String word, Object value) {
        TrieNode currentNode = TIRE_ROOT;
        for (char c : word.toCharArray()) {
            currentNode.children.putIfAbsent(c, new TrieNode());
            currentNode = currentNode.children.get(c);
        }
        currentNode.isWord = true;
        currentNode.word = word;
        //currentNode.value = value;
    }

    public boolean contains(String word) {
        TrieNode currentNode = TIRE_ROOT;
        for (char c : word.toCharArray()) {
            if (!currentNode.children.containsKey(c)) {
                return false;
            }
            currentNode = currentNode.children.get(c);
        }
        return currentNode.isWord;
    }

    public boolean startsWith(String prefix) {
        TrieNode currentNode = TIRE_ROOT;
        for (char c : prefix.toCharArray()) {
            if (!currentNode.children.containsKey(c)) {
                return false;
            }
            currentNode = currentNode.children.get(c);
        }
        return true;
    }

    public List<String> getWordsWithPrefix(String prefix, int limit) {
        List<String> words = new ArrayList<>();
        TrieNode currentNode = TIRE_ROOT;
        for (char c : prefix.toCharArray()) {
            if (!currentNode.children.containsKey(c)) {
                return words;
            }
            currentNode = currentNode.children.get(c);
        }
        getWordsWithPrefixHelper(currentNode, words, limit);
        return words;
    }

    private void getWordsWithPrefixHelper(TrieNode node, List<String> words, int limit) {
        if (node.isWord) {
            words.add(node.word);
            if (words.size() >= limit) {
                return;
            }
        }

        for (char c : node.children.keySet()) {
            getWordsWithPrefixHelper(node.children.get(c), words, limit - words.size());
            if (words.size() >= limit) {
                return;
            }
        }
    }

//    public Object getValue(String word) {
//        TrieNode currentNode = TIRE_ROOT;
//        for (char c : word.toCharArray()) {
//            if (!currentNode.children.containsKey(c)) {
//                return null;
//            }
//            currentNode = currentNode.children.get(c);
//        }
//        return currentNode.value;
//    }
}