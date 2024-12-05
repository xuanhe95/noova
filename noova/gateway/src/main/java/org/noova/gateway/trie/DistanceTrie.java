package org.noova.gateway.trie;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;

public class DistanceTrie extends Trie implements Serializable {
    private static class TrieNode implements Serializable {
        @JsonProperty
        private final Map<Character, TrieNode> children = new HashMap<>();
        @JsonProperty
        private boolean isWord;
        @JsonProperty
        private String word;
        @JsonProperty
        private int editDistance = Integer.MAX_VALUE;
    }

    @JsonProperty
    private final TrieNode TIRE_ROOT;

    public DistanceTrie() {
        TIRE_ROOT = new TrieNode();
    }

    // 插入单词并计算初始编辑距离
    public void insert(String word, Object value) {
        TrieNode currentNode = TIRE_ROOT;
        for (char c : word.toCharArray()) {
            currentNode.children.putIfAbsent(c, new TrieNode());
            currentNode = currentNode.children.get(c);
        }
        currentNode.isWord = true;
        currentNode.word = word;
    }

        // 获取与目标单词编辑距离在最大限制内的词
        public List<String> getWordsWithinEditDistance(String targetWord, int maxEditDistance, int limit) {
            List<String> result = Collections.synchronizedList(new ArrayList<>());
            int[][] dpTable = new int[2][targetWord.length() + 1];

            // 初始化第一行编辑距离
            for (int i = 0; i <= targetWord.length(); i++) {
                dpTable[0][i] = i;
            }

            System.out.println("Searching for: " + targetWord);

//            // 并行搜索顶层节点
//            ForkJoinPool forkJoinPool = new ForkJoinPool();
//            try {
//                forkJoinPool.submit(() -> TIRE_ROOT.children.entrySet().parallelStream().forEach(entry -> {
//                    searchRecursive(entry.getValue(), String.valueOf(entry.getKey()), targetWord, dpTable, maxEditDistance, result, 0, limit);
//                })).get();
//            } catch (InterruptedException | ExecutionException e) {
//                e.printStackTrace();
//            }

            List<Map.Entry<Character, TrieNode>> sortedChildren = new ArrayList<>(TIRE_ROOT.children.entrySet());
            sortedChildren.sort((e1, e2) -> {
                boolean match1 = e1.getKey() == targetWord.charAt(0);
                boolean match2 = e2.getKey() == targetWord.charAt(0);
                return Boolean.compare(match2, match1); // 优先匹配的路径
            });

            for (Map.Entry<Character, TrieNode> entry : sortedChildren) {
                searchRecursive(entry.getValue(), String.valueOf(entry.getKey()), targetWord, dpTable, maxEditDistance, result, 0, limit);
            }



//            if (result.size() > limit) {
//                return result.subList(0, limit);
//            }
            return result;
        }

    private void searchRecursive(TrieNode node, String currentPrefix, String targetWord,
                                 int[][] dpTable, int maxEditDistance, List<String> result, int currentRowIdx, int limit) {
        int columns = targetWord.length() + 1;
        int[] previousRow = dpTable[currentRowIdx];
        int[] currentRow = dpTable[1 - currentRowIdx]; // 使用另一行

        // 初始化当前行编辑距离
        currentRow[0] = previousRow[0] + 1;

        for (int i = 1; i < columns; i++) {
            int insertCost = currentRow[i - 1] + 1;
            int deleteCost = previousRow[i] + 1;
            int replaceCost = (i - 1 < currentPrefix.length() && targetWord.charAt(i - 1) == currentPrefix.charAt(currentPrefix.length() - 1))
                    ? previousRow[i - 1]
                    : previousRow[i - 1] + 1;

            currentRow[i] = Math.min(insertCost, Math.min(deleteCost, replaceCost));
        }

        // 剪枝：如果当前行的最小值已经超过最大允许的编辑距离，停止搜索
        if (Arrays.stream(currentRow).allMatch(d -> d > maxEditDistance)) {
            return;
        }

        // 如果当前节点是单词并且编辑距离在范围内，加入结果
        if (node.isWord && currentRow[columns - 1] <= maxEditDistance) {
            result.add(node.word);
            if (result.size() >= limit) {
                return; // 超过限制时停止
            }
        }

        // 优先搜索可能匹配的路径，并按长度接近排序
        List<Map.Entry<Character, TrieNode>> sortedChildren = new ArrayList<>(node.children.entrySet());
        sortedChildren.sort((e1, e2) -> {
            // 获取当前路径的长度
            int length1 = currentPrefix.length() + 1; // e1 的路径长度
            int length2 = currentPrefix.length() + 1; // e2 的路径长度

            // 计算与目标单词长度的差异
            int diff1 = Math.abs(targetWord.length() - length1);
            int diff2 = Math.abs(targetWord.length() - length2);

            // 如果差异相等，则按字符匹配优先级排序
            if (diff1 == diff2) {
                boolean match1 = targetWord.length() > currentPrefix.length() && e1.getKey() == targetWord.charAt(currentPrefix.length());
                boolean match2 = targetWord.length() > currentPrefix.length() && e2.getKey() == targetWord.charAt(currentPrefix.length());
                return Boolean.compare(match2, match1); // 优先匹配的路径
            }

            // 按长度差异排序
            return Integer.compare(diff1, diff2);
        });

        // 遍历排序后的子节点
        for (Map.Entry<Character, TrieNode> entry : sortedChildren) {
            if (result.size() >= limit) {
                return; // 超过限制时停止
            }
            searchRecursive(entry.getValue(), currentPrefix + entry.getKey(), targetWord, dpTable, maxEditDistance, result, 1 - currentRowIdx, limit);
        }
    }
    private int getMatchedPrefixLength(String prefix, String targetWord) {
        int length = Math.min(prefix.length(), targetWord.length());
        int matched = 0;
        for (int i = 0; i < length; i++) {
            if (prefix.charAt(i) == targetWord.charAt(i)) {
                matched++;
            } else {
                break;
            }
        }
        return matched;
    }
    public List<String> getWordsWithinEditDistancePriority(String targetWord, int maxEditDistance, int limit) {
        List<String> result = new ArrayList<>();
        PriorityQueue<PriorityNode> pq = new PriorityQueue<>(
                Comparator.comparingInt((PriorityNode n) -> n.editDistance) // 按编辑距离排序
                        .thenComparingInt(n -> -getMatchedPrefixLength(n.prefix, targetWord)) // 优先匹配前缀越多
                        .thenComparingInt(n -> Math.abs(n.prefix.length() - targetWord.length())) // 长度接近优先
        );

        // 初始化第一行编辑距离
        int[] initialRow = new int[targetWord.length() + 1];
        for (int i = 0; i <= targetWord.length(); i++) {
            initialRow[i] = i;
        }

        // 将根节点加入队列
        pq.offer(new PriorityNode(TIRE_ROOT, "", initialRow, 0));

        while (!pq.isEmpty() && result.size() < limit) {
            PriorityNode current = pq.poll();
            TrieNode node = current.node;
            String currentPrefix = current.prefix;
            int[] previousRow = current.editDistanceRow;

            // 如果当前节点是单词并且编辑距离在范围内，加入结果
            if (node.isWord && previousRow[targetWord.length()] <= maxEditDistance) {
                result.add(node.word);
            }

            // 遍历当前节点的子节点
            for (Map.Entry<Character, TrieNode> entry : node.children.entrySet()) {
                char c = entry.getKey();
                TrieNode childNode = entry.getValue();

                // 计算当前子节点的编辑距离
                int[] currentRow = new int[targetWord.length() + 1];
                currentRow[0] = previousRow[0] + 1;

                for (int i = 1; i <= targetWord.length(); i++) {
                    int insertCost = currentRow[i - 1] + 1;
                    int deleteCost = previousRow[i] + 1;
                    int replaceCost = (targetWord.charAt(i - 1) == c) ? previousRow[i - 1] : previousRow[i - 1] + 1;

                    currentRow[i] = Math.min(insertCost, Math.min(deleteCost, replaceCost));
                }

                // 剪枝：如果当前行的最小值已经超过最大允许的编辑距离，跳过该子节点
                if (Arrays.stream(currentRow).min().orElse(Integer.MAX_VALUE) > maxEditDistance) {
                    continue;
                }

                // 按优先级将子节点加入队列
                pq.offer(new PriorityNode(childNode, currentPrefix + c, currentRow, currentRow[targetWord.length()]));
            }
        }

        return result;
    }

    // 优先级队列中的节点
    private static class PriorityNode {
        TrieNode node;
        String prefix;
        int[] editDistanceRow;
        int editDistance;

        PriorityNode(TrieNode node, String prefix, int[] editDistanceRow, int editDistance) {
            this.node = node;
            this.prefix = prefix;
            this.editDistanceRow = editDistanceRow;
            this.editDistance = editDistance;
        }
    }


    public List<String> getWordsWithinEditDistanceBFS(String targetWord, int maxEditDistance, int limit) {
        List<String> result = new ArrayList<>();
        Queue<BFSNode> queue = new LinkedList<>();

        // 初始化第一行编辑距离
        int[] initialRow = new int[targetWord.length() + 1];
        for (int i = 0; i <= targetWord.length(); i++) {
            initialRow[i] = i;
        }

        // 将根节点加入队列
        queue.offer(new BFSNode(TIRE_ROOT, "", initialRow));

        while (!queue.isEmpty() && result.size() < limit) {
            BFSNode current = queue.poll();
            TrieNode node = current.node;
            String currentPrefix = current.prefix;
            int[] previousRow = current.editDistanceRow;

            // 如果当前节点是单词并且编辑距离在范围内，加入结果
            if (node.isWord && previousRow[targetWord.length()] <= maxEditDistance) {
                result.add(node.word);
            }

            // 遍历当前节点的子节点
            for (Map.Entry<Character, TrieNode> entry : node.children.entrySet()) {
                char c = entry.getKey();
                TrieNode childNode = entry.getValue();

                // 计算当前子节点的编辑距离
                int[] currentRow = new int[targetWord.length() + 1];
                currentRow[0] = previousRow[0] + 1;

                for (int i = 1; i <= targetWord.length(); i++) {
                    int insertCost = currentRow[i - 1] + 1;
                    int deleteCost = previousRow[i] + 1;
                    int replaceCost = (targetWord.charAt(i - 1) == c) ? previousRow[i - 1] : previousRow[i - 1] + 1;

                    currentRow[i] = Math.min(insertCost, Math.min(deleteCost, replaceCost));
                }

                // 剪枝：如果当前行的最小值已经超过最大允许的编辑距离，跳过该子节点
                if (Arrays.stream(currentRow).min().orElse(Integer.MAX_VALUE) > maxEditDistance) {
                    continue;
                }

                // 按优先级将子节点加入队列
                queue.offer(new BFSNode(childNode, currentPrefix + c, currentRow));
            }
        }

        return result;
    }

    // 辅助类，用于存储 BFS 中的节点状态
    private static class BFSNode {
        TrieNode node;
        String prefix;
        int[] editDistanceRow;

        BFSNode(TrieNode node, String prefix, int[] editDistanceRow) {
            this.node = node;
            this.prefix = prefix;
            this.editDistanceRow = editDistanceRow;
        }
    }

}
