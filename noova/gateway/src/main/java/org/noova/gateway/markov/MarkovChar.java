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

    /**
     * 根据当前的key（n-gram）获取下一个字符的所有可能后继字符列表
     */
    public ArrayList<String> getFollows(String key){
        ArrayList<String> follows = new ArrayList<>();
        // 在文本中查找匹配当前key的所有位置
        for (int i = 0; i < text.length() - key.length(); i++) {
            String curString = text.substring(i, i + key.length());
            if (curString.equals(key)) {
                // 获取key后面的下一个字符（推测是Markov模型的状态转移）
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

    /**
     * 根据当前的prefixLength来生成一个随机文本
     */
    public String getRandomText(int numChars) {
        // 检查文本长度是否足够长，确保可以生成文本
        if (text.length() < prefixLength) {
            return "";  // 如果文本长度小于prefixLength，无法生成文本
        }

        StringBuilder builder = new StringBuilder();

        // 随机选择一个合法的起始位置
        int index = random.nextInt(text.length() - prefixLength + 1);  // 确保index不会越界
        String key = text.substring(index, index + prefixLength);  // 获取prefixLength长度的起始字符串
        builder.append(key);

        // 生成随机文本
        for (int k = 0; k < numChars - prefixLength; k++) {
            // 获取当前key的后续字符列表
            ArrayList<String> follows = getFollows(key);
            if (follows.isEmpty()) {
                break;  // 如果没有后续字符，停止生成
            }

            // 随机选择后续字符
            String nextChar = follows.get(random.nextInt(follows.size()));
            builder.append(nextChar);

            // 更新key，滑动窗口操作
            key = key.substring(1) + nextChar;  // 确保滑动窗口的更新不会越界
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
        // 检查文本长度是否足够长，确保可以生成文本
        if (text.length() < prefixLength) {
            return;  // 如果文本长度小于prefixLength，无法生成文本
        }

        StringBuilder builder = new StringBuilder(prefix);

        // 生成随机文本
        for (int k = 0; k < numChars - prefixLength; k++) {
            // 获取当前key的后续字符列表
            ArrayList<String> follows = getFollows(prefix);
            if (follows.isEmpty()) {
                break;  // 如果没有后续字符，停止生成
            }

            // 随机选择后续字符
            String nextChar = follows.get(random.nextInt(follows.size()));
            builder.append(nextChar);

            String nextWord = builder.toString();

            // 更新key，滑动窗口操作
            prefix = prefix.substring(1) + nextChar;  // 确保滑动窗口的更新不会越界
            if(words.contains(nextWord)){
            }
            else if(trie != null && trie.contains(nextWord)){
                words.add(nextWord);
            }

            getRandomTextWithPrefix(prefix, words, numChars-k, stackLevel-1);
        }

    }



    public String getRandomTextWithPrefix(String prefix, int numChars) {
        // 检查文本长度是否足够长，确保可以生成文本
        if (text.length() < prefixLength) {
            return "";  // 如果文本长度小于prefixLength，无法生成文本
        }

        StringBuilder builder = new StringBuilder(prefix);

        // 生成随机文本
        for (int k = 0; k < numChars - prefixLength; k++) {
            // 获取当前key的后续字符列表
            ArrayList<String> follows = getFollows(prefix);
            if (follows.isEmpty()) {
                break;  // 如果没有后续字符，停止生成
            }

            // 随机选择后续字符
            String nextChar = follows.get(random.nextInt(follows.size()));
            builder.append(nextChar);

            // 更新key，滑动窗口操作
            prefix = prefix.substring(1) + nextChar;  // 确保滑动窗口的更新不会越界
        }

        return builder.toString();

    }

}
