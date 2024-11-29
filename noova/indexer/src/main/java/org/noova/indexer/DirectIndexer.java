package org.noova.indexer;

import opennlp.tools.lemmatizer.DictionaryLemmatizer;
import opennlp.tools.lemmatizer.Lemmatizer;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTagger;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;
import org.noova.kvs.KVS;
import org.noova.kvs.KVSClient;
import org.noova.kvs.Row;
import org.noova.tools.Hasher;
import org.noova.tools.Logger;
import org.noova.tools.PropertyLoader;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.time.LocalDateTime;


public class DirectIndexer {

    private static final Logger log = Logger.getLogger(DirectIndexer.class);
    private static final boolean ENABLE_PARSE_ENTITY = false;
    private static final String DELIMITER = PropertyLoader.getProperty("delimiter.default");
    private static final String CRAWL_TABLE = PropertyLoader.getProperty("table.crawler");
    private static final String CRAWL_URL = PropertyLoader.getProperty("table.crawler.url");
    private static final String CRAWL_TEXT = PropertyLoader.getProperty("table.crawler.text");
    private static final String CRAWL_IP = PropertyLoader.getProperty("table.crawler.ip");
    private static final String CRAWL_IMAGES = PropertyLoader.getProperty("table.crawler.images");
    private static final String INDEX_TABLE = PropertyLoader.getProperty("table.index");
    private static final String INDEX_LINKS = PropertyLoader.getProperty("table.index.links");
    private static final String INDEX_IMAGES = PropertyLoader.getProperty("table.index.images");
    private static final String URL_ID_TABLE = PropertyLoader.getProperty("table.url");
    private static final String URL_ID_VALUE = PropertyLoader.getProperty("table.url.id");
    private static final String GLOBAL_COUNTER_TABLE = "pt-count";
    private static final String GLOBAL_COUNTER_KEY = "global_counter";

    static int pageCount = 0;
    static Queue<String> pageDetails = new ConcurrentLinkedQueue<>();
    static final Map<String, String> URL_ID_CACHE = new HashMap<>();

    private static class WordStats {
        int frequency = 0;
        int firstLocation = 0;
    }

    private static final TokenizerModel tokenizerModel;
    private static final POSModel posModel;
    private static final Tokenizer tokenizer;
    private static final POSTagger posTagger;
    private static final Lemmatizer lemmatizer;
    private static final Set<String> stopWords;
    private static NameFinderME personNameFinder;
    private static NameFinderME locationNameFinder;

    private static final Set<String> INVALID_POS = Set.of(
            "DT",  // determiner (the, a, an)
            "IN",  // preposition (in, of, at)
            "CC",  // conjunction (and, or, but)
            "UH",  // interjection (oh, wow, ah)
            "FW",  // Foreign Word
            "SYM", // Symbol
            "LS",  // List item marker
            "PRP", // Personal pronouns
            "WDT", // Wh-determiner
            "WP",  // Wh-pronoun
            "WRB"  // Wh-adverb
    );

    static {
        try {
            log.info("Starting to load OpenNLP models...");

            InputStream tokenStream = DirectIndexer.class.getResourceAsStream("/models/en-token.bin");
            InputStream posStream = DirectIndexer.class.getResourceAsStream("/models/en-pos-maxent.bin");
            InputStream dictStream = DirectIndexer.class.getResourceAsStream("/models/en-lemmatizer.dict.txt");

            if (tokenStream == null) {
                throw new IOException("Tokenizer model not found in resources: /models/en-token.bin");
            }
            if (posStream == null) {
                throw new IOException("POS model not found in resources: /models/en-pos-maxent.bin");
            }
            if (dictStream == null) {
                throw new IOException("can not find：/models/dictionary.txt");
            }

            log.info("Loading tokenizer model...");
            tokenizerModel = new TokenizerModel(tokenStream);

            log.info("Loading POS model...");
            posModel = new POSModel(posStream);

            tokenizer = new TokenizerME(tokenizerModel);
            posTagger = new POSTaggerME(posModel);
            lemmatizer = new DictionaryLemmatizer(dictStream);

            log.info("Loading stopwords...");
            stopWords = new HashSet<>();
            try (InputStream is = DirectIndexer.class.getResourceAsStream("/models/stopwords-en.txt")) {
                if (is == null) {
                    log.warn("Warning: stopwords-en.txt not found in resources");
                } else {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (!line.trim().isEmpty()) {
                            stopWords.add(line.trim().toLowerCase());
                        }
                    }
                }
            }

            log.info("All models loaded successfully");
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize OpenNLP models: " + e.getMessage(), e);
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        KVS kvs = new KVSClient(PropertyLoader.getProperty("kvs.host") + ":" + PropertyLoader.getProperty("kvs.port"));

        String startKey = null;
        String endKey = null;
        if(args.length == 1){
            startKey = args[0];
        } else if(args.length > 1){
            startKey = args[0];
            endKey = args[1];
        } else{
            System.out.println("No key range specified, scan all tables");
        }

        System.out.println("Key range: " + startKey + " - " + endKey);

        long start = System.currentTimeMillis();
        System.out.println("Start indexing");
        Iterator<Row> pages = null;
        try {
            pages = kvs.scan(CRAWL_TABLE, startKey, endKey);
        } catch (IOException e) {
            System.out.println("Error: " + e.getMessage());
            return;
        }

        Iterator<Row> indexes = null;
        try {
            indexes = kvs.scan(INDEX_TABLE, null, null);
        } catch (IOException e) {
            System.out.println("Error: " + e.getMessage());
        }

        // load url id to the cache
        System.out.println("Loading URL ID...");
        loadUrlId(kvs);
        System.out.println("URL ID loaded");
        generateInvertedIndexBatch(kvs, pages, indexes);

        long end = System.currentTimeMillis();

        System.out.println("Time: " + (end - start) + "ms");

    }

    private static void processPage(Row page,
                                    Map<String, Map<String, WordStats>> wordMap,
                                    Map<String, StringBuffer> imageMap,
                                    Map<String, Map<String, WordStats>> namedEntityMap,
                                    KVS kvs) throws InterruptedException{
        pageCount++;
        pageDetails.add(page.key()+"\n");

        String url = page.get(CRAWL_URL);
        String text = page.get(CRAWL_TEXT);
        //String ip = page.get(CRAWL_IP);
        String images = page.get(CRAWL_IMAGES);

        // skip op-heavy nlp tasks if url DNE
        if (url == null || url.isEmpty()) {
            System.out.println("Skipping row with no URL: " + page.key());
            return;
        }

        // store urlId instead of url
        String urlId;
        try {
            urlId = getUrlId(kvs, url);
        } catch (IOException e) {
            System.err.println("Error fetching/assigning ID for URL: " + url + ", " + e.getMessage());
            return;
        }

        // normalize words (lemmatize + stop word rm) and populate wordMap
        long startTime = System.currentTimeMillis();
        String[] words = normalizeWord(text);
        long endTime = System.currentTimeMillis();
        long duration = (endTime - startTime);
        System.out.println("Time to normalize words: " + duration + "ms");
        for(int i = 0; i < words.length; i++){
            String word = words[i];
            if(word == null || word.isEmpty()){
                continue;
            }
            wordMap.putIfAbsent(word, new ConcurrentHashMap<>());
            Map<String, WordStats> urlStatsMap = wordMap.get(word);

            //System.out.println("Word: " + word + " URL: " + urlId);

            if (!urlStatsMap.containsKey(urlId)) {
                WordStats stats = new WordStats();
                stats.frequency = 1;
                stats.firstLocation = i;
                urlStatsMap.put(urlId, stats);
            } else {
                WordStats stats = urlStatsMap.get(urlId);
                stats.frequency++;
            }
        }

        // parse named entities
        if(ENABLE_PARSE_ENTITY){
            extractNamedEntities(text, urlId, namedEntityMap);
        }

        // parse images and populate imageMap
        Map<String, Set<String>> wordImageMap = parseImages(images);
        for(Map.Entry<String, Set<String>> entry : wordImageMap.entrySet()){
            String word = entry.getKey();
            Set<String> imageSet = entry.getValue();
            StringBuffer buffer = imageMap.computeIfAbsent(word, k -> new StringBuffer());
            for(String image : imageSet){
                if (!buffer.toString().contains(image)) {
                    buffer.append(image).append(DELIMITER);
                }
            }
        }
    }

    private static void extractNamedEntities(String text, String urlId, Map<String, Map<String, WordStats>> namedEntityMap) {
        // helper to extract entity in a page

        if (!ENABLE_PARSE_ENTITY || text == null || text.trim().isEmpty()) {
            return; // skip ner processing
        }

        if (personNameFinder == null || locationNameFinder == null) {
            throw new IllegalStateException("NER models not initialized. Ensure ENABLE_PARSE_ENTITY is true and models are loaded.");
        }

        String[] tokens = tokenizer.tokenize(text);

        // parse person entities
        Span[] personSpans = personNameFinder.find(tokens);
        processNamedEntitySpans(personSpans, tokens, urlId, namedEntityMap);

        // parse location entities
        Span[] locationSpans = locationNameFinder.find(tokens);
        processNamedEntitySpans(locationSpans, tokens, urlId, namedEntityMap);
    }

    private static void processNamedEntitySpans(Span[] spans, String[] tokens, String urlId, Map<String, Map<String, WordStats>> namedEntityMap) {
        // helper to populate entity stats
        for (int i = 0; i<spans.length; i++) {
            Span span = spans[i];
            String namedEntity = String.join(" ", Arrays.copyOfRange(tokens, span.getStart(), span.getEnd())).toLowerCase();
            namedEntityMap.putIfAbsent(namedEntity, new ConcurrentHashMap<>());
            Map<String, WordStats> urlStatsMap = namedEntityMap.get(namedEntity);

            if (!urlStatsMap.containsKey(urlId)) {
                WordStats stats = new WordStats();
                stats.frequency = 1;
                stats.firstLocation = i;
                urlStatsMap.put(urlId, stats);
            } else {
                WordStats stats = urlStatsMap.get(urlId);
                stats.frequency++;
            }
        }
    }


    private static void generateInvertedIndexBatch(KVS kvs, Iterator<Row> pages, Iterator<Row> indexes) throws InterruptedException, IOException {
        Map<String, Map<String, WordStats>> wordMap = new ConcurrentHashMap<>();
        Map<String, StringBuffer> imageMap = new ConcurrentHashMap<>();
        Map<String, Map<String, WordStats>> namedEntityMap= new ConcurrentHashMap<>();

//        // option 1. use threadpool
//        FixedThreadPool threadPool = new FixedThreadPool(100);
//
//
//        while(pages != null && pages.hasNext()) {
//            Row page = pages.next();
//            threadPool.execute(() -> {
//                try {
//                    processPage(page, wordMap, imageMap, namedEntityMap, kvs);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//            });
//        }
//        threadPool.waitForCompletion();

        // option 2. dynamically sized pool
//        ForkJoinPool forkJoinPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
//
//        forkJoinPool.submit(() -> pages.forEachRemaining(page -> {
//            try {
//                processPage(page, wordMap, imageMap, namedEntityMap, kvs);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//        })).join();

        // option 3. sequential
        pages.forEachRemaining(page -> {
            try {
                processPage(page , wordMap , imageMap , namedEntityMap, kvs);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        try (BufferedWriter pageWriter = new BufferedWriter(new FileWriter("pages_processed.txt", true))) {
            for (String detail : pageDetails) {
                pageWriter.write(detail);
            }
        } catch (IOException e) {
            System.err.println("Error writing pages to file: " + e.getMessage());
        }

        System.out.println("Total pages processed: " + pageCount);

        saveIndexToTable(kvs, wordMap, imageMap, namedEntityMap);
    }

    private static void saveIndexToTable (KVS kvs, 
                                          Map<String, Map<String, WordStats>> wordMap, 
                                          Map<String, StringBuffer> imageMap,
                                          Map<String, Map<String, WordStats>> namedEntityMap) throws IOException {
        Set<String> mergedWords = new HashSet<>(wordMap.keySet());
        mergedWords.addAll(imageMap.keySet());

        var count = 1;
        long lastTime = System.nanoTime();
//        AtomicInteger count = new AtomicInteger(1);
//        AtomicLong lastTime = new AtomicLong(System.nanoTime());
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        for(String word : mergedWords) {
//        mergedWords.parallelStream().forEach(word->{
            Map<String, WordStats> urlStatsMap = wordMap.get(word);
            String images = imageMap.get(word) == null ? "" : imageMap.get(word).toString();

            Row row = null;
            try {
                row = kvs.getRow(INDEX_TABLE , word);
            } catch (IOException e) {
                System.out.println("Error: " + e.getMessage());
            }
            if (row == null) {
                row = new Row(word);
            }
            if (row.key().isEmpty()) {
                System.out.println("row key " + row.key());
            }

            String imgs = row.get(INDEX_IMAGES);

            if (urlStatsMap != null) {
                StringBuilder linksBuilder = new StringBuilder();
                for (Map.Entry<String, WordStats> entry : urlStatsMap.entrySet()) {
                    if (!linksBuilder.isEmpty()) {
                        linksBuilder.append(DELIMITER);
                    }
                    String urlId = entry.getKey(); // use urlID instead of full url
                    WordStats stats = entry.getValue();
                    linksBuilder.append(urlId).append(":").append(stats.frequency).append(",").append(stats.firstLocation);
                }

                String existingLinks = row.get(INDEX_LINKS);
                if (existingLinks != null && !existingLinks.isEmpty()) {
                    linksBuilder.insert(0 , existingLinks + DELIMITER);
                }

                row.put(INDEX_LINKS , linksBuilder.toString());
            }

            if (imgs == null) {
                imgs = images;
            } else {
                imgs = imgs + DELIMITER + images;
            }
            row.put(INDEX_IMAGES , imgs);

            try {

                count++;
                if (count % 500 == 0) {
                    int remainder = count % 1000;
                    long currentTime = System.nanoTime();
                    double deltaTime = (currentTime - lastTime) / 1_000_000.0;
                    String formattedTime = LocalDateTime.now().format(formatter);
                    System.out.printf("Count: %d, %% 1000: %d, Time: %s, Delta Time: %.6f ms%n" ,
                            count , remainder , formattedTime , deltaTime);
                    lastTime=currentTime;
                }
                kvs.putRow(INDEX_TABLE , row);
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());

            }
//        });
        }


        var countEntity =1;
        long lastTimeEntity = System.nanoTime();
        for(String entity : namedEntityMap.keySet()) {
            Map<String, WordStats> urlStatsMap = namedEntityMap.get(entity);

            Row row = null;
            try {
                row = kvs.getRow(INDEX_TABLE , entity);
            } catch (IOException e) {
                System.out.println("Error: " + e.getMessage());
            }
            if (row == null) {
                row = new Row(entity);
            }
            if (row.key().isEmpty()) {
                System.out.println("row key " + row.key());
            }
            

            if (urlStatsMap != null) {
                StringBuilder linksBuilder = new StringBuilder();
                for (Map.Entry<String, WordStats> entry : urlStatsMap.entrySet()) {
                    if (!linksBuilder.isEmpty()) {
                        linksBuilder.append(DELIMITER);
                    }
                    String urlId = entry.getKey(); // use urlID instead of full url
                    WordStats stats = entry.getValue();
                    linksBuilder.append(urlId).append(":").append(stats.frequency).append(",").append(stats.firstLocation);
                }

                String existingLinks = row.get(INDEX_LINKS);
                if (existingLinks != null && !existingLinks.isEmpty()) {
                    linksBuilder.insert(0 , existingLinks + DELIMITER);
                }

                row.put(INDEX_LINKS , linksBuilder.toString());
            }

            try {

                countEntity++;
                if (countEntity % 500 == 0) {
                    int remainder = countEntity % 1000;
                    long currentTime = System.nanoTime();
                    double deltaTime = (currentTime - lastTimeEntity) / 1_000_000.0;
                    String formattedTime = LocalDateTime.now().format(formatter);
                    System.out.printf("Count: %d, %% 1000: %d, Time: %s, Delta Time: %.6f ms%n" ,
                            countEntity , remainder , formattedTime , deltaTime);
                    lastTimeEntity=currentTime;
                }
                kvs.putRow(INDEX_TABLE , row);
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());

            }
        }
    }

    private static void loadUrlId(KVS kvs) throws IOException {
        var ids = kvs.scan(URL_ID_TABLE, null, null);
        ids.forEachRemaining(row -> {
            String id = row.get(URL_ID_VALUE);
            if(id == null){
                return;
            }
            URL_ID_CACHE.put(row.key(), id);
        });
    }


    private static String getUrlId(KVS kvs, String url) throws IOException {

        if(URL_ID_CACHE.containsKey(url)){
            System.out.println("URL_ID_CACHE contains url: " + url);
            return URL_ID_CACHE.get(url);
        }

        // use URL_ID_TABLE to find an url's corresponding urlID
        Row row = kvs.getRow(URL_ID_TABLE, Hasher.hash(url));
        if (row != null) {
            String id = row.get(URL_ID_VALUE);
            URL_ID_CACHE.put(url, id);
            return id;
        }

        // generate new id if url DNE
        synchronized (GLOBAL_COUNTER_KEY) {
            Row counterRow = kvs.getRow(GLOBAL_COUNTER_TABLE, GLOBAL_COUNTER_KEY);
            int globalCounter = (counterRow != null) ? Integer.parseInt(counterRow.get(URL_ID_VALUE)) : 0;

            // update global counter
            globalCounter++;
            if (counterRow == null) {
                counterRow = new Row(GLOBAL_COUNTER_KEY);
            }
            counterRow.put(URL_ID_VALUE, String.valueOf(globalCounter));
            kvs.putRow(GLOBAL_COUNTER_TABLE, counterRow);

            // store url and global counter to URL_ID_TABLE
            Row urlRow = new Row(Hasher.hash(url));
            urlRow.put(URL_ID_VALUE, String.valueOf(globalCounter));
            kvs.putRow(URL_ID_TABLE, urlRow);
            URL_ID_CACHE.put(url, String.valueOf(globalCounter));

            return String.valueOf(globalCounter);
        }
    }

    public static String[] normalizeWord(String text) {
        if (text == null) {
            return new String[0];
        }
        try {
            String filteredText = filterPage(text);
            if (filteredText.isEmpty()) {
                return new String[0];
            }

            String[] tokens = tokenizer.tokenize(filteredText);
            if (tokens.length == 0) {
                return new String[0];
            }

            String[] tags = posTagger.tag(tokens);
            if (tags.length == 0) {
                return new String[0];
            }

            String[] lemmas = lemmatizer.lemmatize(tokens, tags);

            List<String> validWords = new ArrayList<>();
            for (int i = 0; i < lemmas.length; i++) {
                String lemma = lemmas[i].toLowerCase();
                if (!lemma.isEmpty() && isValidWord(lemma, tags[i]) && !stopWords.contains(lemma)) {
                    validWords.add(lemma);
                }
            }

            return validWords.toArray(new String[0]);
        } catch (Exception e) {
            log.error("Error normalizing text: " + text, e);
            return new String[0];
        }
    }

    public static boolean isValidWord(String word, String pos) {
        if (word.length() <= 2) {
            return false;
        }

        if (!word.matches("^[a-zA-Z]+$")) {
            return false;
        }

        if (word.matches(".*(.)\\1{2,}.*")) {
            return false;
        }

        if (INVALID_POS.contains(pos)) {
            return false;
        }

        return true;
    }

    public static String filterPage(String page) {
        if(page == null) return "";
        return page.toLowerCase()
                .strip()
                .replaceAll("<[^>]*>", " ")
                .strip()
                .replaceAll("[.,:;!?''\"()\\-\\r\\n\\t]", " ")
                .strip()
                .replaceAll("[^\\p{L}\\s]", " ")
                .strip();
    }

    static Map<String, Set<String>> parseImages(String images) {
        if(images == null || images.isEmpty()){
            return new HashMap<>();
        }
        Map<String, Set<String>> wordImageMap = new HashMap<>();
        String[] rawImages = images.split("\n");
        for(String rawImage : rawImages){
            String alt = extractImage(rawImage, "alt");
            // only save the src of the image
            String src = extractImage(rawImage, "src");
            if(alt == null || alt.isEmpty() || src == null || src.isEmpty()){
                continue;
            }

            String[] words = normalizeWord(alt);

            for(String word : words){
                if(word == null || word.isBlank()){
                    continue;
                }
                Set<String> imageSet = wordImageMap.computeIfAbsent(word, k -> new HashSet<>());
                imageSet.add(src);
            }
        }
        return wordImageMap;
    }

    static String extractImage(String html, String tag) {
        if(html == null || html.isEmpty()){
            return "";
        }
        String regex = tag + "\\s*=\\s*\"([^\"]*)\"";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(html);

        if (matcher.find()) {
            // return alt
            return matcher.group(1);
        }
        return "";
    }
}