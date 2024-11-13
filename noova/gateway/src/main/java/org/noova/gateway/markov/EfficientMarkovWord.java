package org.noova.gateway.markov;

import java.util.*;

/**
 * @author Xuanhe Zhang
 */
public class EfficientMarkovWord implements IMarkovModel {
    private String[] myText;
    Random myRandom;
    private final int myOrder;
    private final HashMap<Integer, List<String>> N_GRAM_TRANSITIONS_MAP;


    public EfficientMarkovWord(int order) {
        myRandom = new Random();
        myOrder = order;
        N_GRAM_TRANSITIONS_MAP = new HashMap<>();
    }

    public void setRandom(int seed) {
        myRandom = new Random(seed);
    }

    public void setTraining(String text){
        myText = text.split("\\s+");
        newBuildMap();
        //buildMap();
        //printHashMapInfo();
    }

    private int indexOf(String[] words,WordGram target,int start){
        String[] source = new String[myOrder];
        System.arraycopy(words, start, source, 0, myOrder);
        WordGram wordGram = new WordGram(source,0,myOrder);

        for(int i=start;i<words.length-myOrder;i++){
            if(wordGram.equals(target)){
                return i;
            }
            wordGram = wordGram.shiftAdd(words[i+myOrder]);
        }
        return -1;
    }

    private List<String> getFollows(WordGram kGram) {
        return N_GRAM_TRANSITIONS_MAP.getOrDefault(kGram.hashCode(), Collections.emptyList());
    }

    public String newGetRandomText(int numWords){
        StringBuilder sb = new StringBuilder();
        int startIndex = myRandom.nextInt(myText.length - myOrder);  // 随机选择起始索引

        // 构建起始 WordGram
        WordGram wg = new WordGram(Arrays.copyOfRange(myText, startIndex, startIndex + myOrder), 0, myOrder);
        sb.append(wg.toString()).append(" ");  // 将起始 n-gram 添加到输出文本中

        // 生成随机文本
        for (int k = 0; k < numWords - myOrder; k++) {
            List<String> follows = getFollows(wg);
            if (follows.isEmpty()) {
                    break;
            }

            String nextWord = follows.get(myRandom.nextInt(follows.size()));  // 随机选择下一个单词
            sb.append(nextWord).append(" ");
            wg = wg.shiftAdd(nextWord);  // 更新 WordGram
        }

        return sb.toString().trim();
    }


    public Set<String> getRandomRecommendations(String[] inputWords, int numRecommendations) {
        WordGram inputGram = new WordGram(inputWords, 0, myOrder);
        List<String> follows = getFollows(inputGram);

        // 如果没有后继单词，返回空列表
        if (follows.isEmpty()) {
            return Collections.emptySet();
        }

        Set<String> recommendations = new HashSet<>();

        // 随机选择多个推荐单词
//        for (int i = 0; i < numRecommendations; i++) {
//            String recommendation = follows.get(myRandom.nextInt(follows.size()));
//            recommendations.add(recommendation);
//        }

        recommendations.addAll(follows);

        return recommendations;
    }

//    public String getRandomTextWithPrefix(String prefix, int numWords){
//        StringBuilder sb = new StringBuilder();
//        String[] rawSource = prefix.split(" ");
//
//        WordGram wg = new WordGram(source,0,myOrder);
//        sb.append(wg.toString()).append(" ");
//        for(int k=0; k < numWords-myOrder; k++){
//            List<String> follows = getFollows(wg);
//            if (follows.size() == 0) {
//                break;
//            }
//            int index = myRandom.nextInt(follows.size());
//            String next = follows.get(index);
//            sb.append(next);
//            sb.append(" ");
//            wg = wg.shiftAdd(next);
//        }
//
//        return sb.toString().trim();
//    }

    public String getRandomText(int numWords){
        StringBuilder sb = new StringBuilder();
        int index = myRandom.nextInt(myText.length  -myOrder);  // random word to start with

        for(int i=0;i<myOrder;i++){   // caltulate first myOrder-th words
            String key = myText[index+i];
            sb.append(key);
            sb.append(" ");
        }
        String[] source=sb.toString().split(" ");
        WordGram wg = new WordGram(source,0,myOrder);   // initialize wg
        for(int k=0; k < numWords-myOrder; k++){
            List<String> follows = getFollows(wg);
            if (follows.size() == 0) {
                break;
            }
            index = myRandom.nextInt(follows.size());
            String next = follows.get(index);
            sb.append(next);
            sb.append(" ");
            wg = wg.shiftAdd(next);
        }

        return sb.toString().trim();
    }

    public void newBuildMap(){
        for (int i = 0; i <= myText.length - myOrder; i++) {
            // 提取当前的 n-gram 词组
            String[] source = new String[myOrder];
            System.arraycopy(myText, i, source, 0, myOrder);
            WordGram wg = new WordGram(source, 0, myOrder);
            Integer hashCode = wg.hashCode();

            // 获取当前 n-gram 的后继单词
            String nextWord = (i + myOrder < myText.length) ? myText[i + myOrder] : null;

            // 从 N_GRAM_TRANSITIONS_MAP 中获取当前 n-gram 的后继列表，若不存在则新建一个
            List<String> follows = N_GRAM_TRANSITIONS_MAP.computeIfAbsent(hashCode, k -> new ArrayList<>());

            // 如果有后继单词，则添加到后继列表中
            if (nextWord != null) {
                follows.add(nextWord);
            }

            // 更新当前 WordGram 对象，生成新的 n-gram
            if (nextWord != null) {
                wg = wg.shiftAdd(nextWord);
            }
        }
    }

    public void buildMap(){
        for(int i=0;i<myText.length-myOrder+1;i++){
            String[] source = new String[myOrder];
            System.arraycopy(myText, i, source, 0, myOrder);

            // create a new WordGram
            WordGram wg = new WordGram(source,0,myOrder);;
            Integer hashCode = wg.hashCode();

            // get the next word
            String nextWord = "";
            if(i+myOrder < myText.length){
                nextWord = myText[i+myOrder];
                if(!N_GRAM_TRANSITIONS_MAP.containsKey(hashCode)){
                    ArrayList<String> follows = new ArrayList<String>();

                    follows.add(nextWord);
                    N_GRAM_TRANSITIONS_MAP.put(hashCode,follows);
                    wg = wg.shiftAdd(nextWord);
                }
                else{
                    List<String> follows = N_GRAM_TRANSITIONS_MAP.get(hashCode);
                    follows.add(nextWord);
                    N_GRAM_TRANSITIONS_MAP.put(hashCode,follows);
                    wg = wg.shiftAdd(nextWord);
                }
            }
            else if(!N_GRAM_TRANSITIONS_MAP.containsKey(hashCode)){
                ArrayList<String> follows = new ArrayList<String>();
                N_GRAM_TRANSITIONS_MAP.put(hashCode,follows);
            }
        }
    }

    public void printHashMapInfo(){
        if(N_GRAM_TRANSITIONS_MAP.size()<50){
            for(Integer hashCode: N_GRAM_TRANSITIONS_MAP.keySet()){
                System.out.println("HashCode: "+hashCode+" "+ N_GRAM_TRANSITIONS_MAP.get(hashCode));
            }
        }
        System.out.println("The number of keys is: " + N_GRAM_TRANSITIONS_MAP.size());
        int maxSize=0;
        Integer maxKey=0;

        for(Integer hashCode: N_GRAM_TRANSITIONS_MAP.keySet()){
            int curSize = N_GRAM_TRANSITIONS_MAP.get(hashCode).size();
            if(curSize>maxSize){
                maxSize = curSize;
                maxKey = hashCode;
            }
        }

        System.out.println("The maximum number of elements following a key is: "+maxSize);
    }

}
