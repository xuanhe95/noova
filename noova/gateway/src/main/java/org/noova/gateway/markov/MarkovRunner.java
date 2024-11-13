package org.noova.gateway.markov;

import org.noova.gateway.storage.StorageStrategy;
import org.noova.gateway.trie.TrieManager;
import org.noova.tools.Logger;
import org.noova.tools.PropertyLoader;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class MarkovRunner {
    private static final Logger log = Logger.getLogger(MarkovRunner.class);

    StorageStrategy storageStrategy = StorageStrategy.getInstance();

    private void train(int numOfWords) throws IOException {
        EfficientMarkovWord markov = new EfficientMarkovWord(1);
        MarkovChar markovChar = new MarkovChar(3, TrieManager.getInstance().buildTrie(PropertyLoader.getProperty("table.index")));
        log.info("[markov] Scanning storage");

        var it = storageStrategy.scan(PropertyLoader.getProperty("table.crawler"));
        if(it == null){
            log.error("[markov] No data found in storage");
            return;
        }
        int count = 0;

        StringBuilder builder = new StringBuilder();

        while (it.hasNext()) {
            log.info("[markov] Training markov model with: " + count++);
            var row = it.next();
            String page = row.get(PropertyLoader.getProperty("table.crawler.page"));
            builder.append(page).append(" ");
            markov.setTraining(page);
            String word = markov.getRandomText(numOfWords);

            System.out.println(word);
        }

        System.out.println("===========");

        String[] words = normalizePage(builder.toString());

        String allWords = String.join(" ", words);

        markovChar.setTraining(allWords);
        String word = markovChar.getRandomText(numOfWords);
        System.out.println(word);

        log.info("[markov] Training complete");
    }

    private void getRandomRecommendations(int order, int numOfWords) throws IOException {
        EfficientMarkovWord markov = new EfficientMarkovWord(order);
        var it = storageStrategy.scan(PropertyLoader.getProperty("table.crawler"));
        if(it == null){
            log.error("[markov] No data found in storage");
            return;
        }
        int count = 0;

        StringBuilder builder = new StringBuilder();
        while (it.hasNext()) {
            log.info("[markov] Training markov model with: " + count++);
            var row = it.next();

            String page = row.get(PropertyLoader.getProperty("table.crawler.page"));
            builder.append(page).append(" ");

        }

        Random random = new Random();
        String[] words = normalizePage(builder.toString());

        String normalizedWords = String.join(" ", words);

        markov.setTraining(normalizedWords);

        String[] allWords = normalizedWords.split(" ");

        int randomIndex = random.nextInt(allWords.length - order);

        String[] inputWords = new String[order];

        for(int i = 0; i < order; i++){
            inputWords[i] = allWords[randomIndex + i];
        }
        //System.out.println(Arrays.toString(words));

        Set<String> wordSet = markov.getRandomRecommendations(inputWords, numOfWords);
        System.out.println("===========");
        System.out.println("Random word: " + Arrays.toString(inputWords));
        System.out.println(wordSet);
    }

    private static String[] normalizePage(String page){
        String noHtml = page.replaceAll("<[^>]*>", " ").strip();


        String noPunctuation = noHtml.replaceAll("[.,:;!?'’\"()\\-\\r\\n\\t]", " ").strip();


        String lowerCase = noPunctuation.toLowerCase().strip();

        String[] words = lowerCase.split(" +");
        return words;
    }

    private void trainWords(int numOfWords) throws IOException {
        MarkovChar markov = new MarkovChar(2, TrieManager.getInstance().buildTrie(PropertyLoader.getProperty("table.index")));
        log.info("[markov] Scanning storage");

        var it = storageStrategy.scan(PropertyLoader.getProperty("table.index"));
        if(it == null){
            log.error("[markov] No data found in storage");
            return;
        }
        int count = 0;
        while (it.hasNext()) {
            log.info("[markov] Training markov model with: " + count++);
            var row = it.next();
            markov.setTraining(row.key());
            log.info("[markov] Training markov model with: " + row.get(PropertyLoader.getProperty("table.index.word")));
            String word = markov.getRandomText(numOfWords);
            System.out.println(word);
        }
        log.info("[markov] Training complete");


    }
    public static void main(String[] args) {
        MarkovRunner markovRun = new MarkovRunner();

        try {
            markovRun.train(10);
            markovRun.getRandomRecommendations(2, 10);
            //markovRun.trainWords(10);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
