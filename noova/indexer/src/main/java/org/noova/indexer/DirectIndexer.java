package org.noova.indexer;

import opennlp.tools.lemmatizer.DictionaryLemmatizer;
import opennlp.tools.lemmatizer.Lemmatizer;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
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
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.time.LocalDateTime;


public class DirectIndexer {

    private static final Logger log = Logger.getLogger(DirectIndexer.class);
    private static final boolean ENABLE_PARSE_ENTITY = true;
    private static final String DELIMITER = PropertyLoader.getProperty("delimiter.default");
    private static final String CRAWL_TABLE = PropertyLoader.getProperty("table.crawler");
    private static final String CRAWL_URL = PropertyLoader.getProperty("table.crawler.url");
    private static final String CRAWL_TEXT = PropertyLoader.getProperty("table.crawler.text");
    private static final String CRAWL_IP = PropertyLoader.getProperty("table.crawler.ip");
    private static final String CRAWL_IMAGES = PropertyLoader.getProperty("table.crawler.images");
    private static final String INDEX_TABLE = PropertyLoader.getProperty("table.index");
    private static final String INDEX_ENTITY_TABLE = PropertyLoader.getProperty("table.index.entity");
    private static final String INDEX_LINKS = PropertyLoader.getProperty("table.index.links");
    private static final String INDEX_IMAGES = PropertyLoader.getProperty("table.index.images");
    private static final String URL_ID_TABLE = PropertyLoader.getProperty("table.url-id");
    private static final String URL_ID_VALUE = PropertyLoader.getProperty("table.url-id.id");
    static int pageCount = 0;
    static Queue<String> pageDetails = new ConcurrentLinkedQueue<>();
    static final Map<String, String> URL_ID_CACHE = new WeakHashMap<>();
    private static final Map<String, String> wordCache = new ConcurrentHashMap<>();

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
    private static NameFinderME organizationNameFinder;

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
                throw new IOException("can not find：/models/en-lemmatizer.dict.txt");
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

            if (ENABLE_PARSE_ENTITY) {
                log.info("Loading NER models...");
                InputStream personModelStream = DirectIndexer.class.getResourceAsStream("/models/en-ner-person.bin");
                InputStream locationModelStream = DirectIndexer.class.getResourceAsStream("/models/en-ner-location.bin");
                InputStream organizationModelStream = DirectIndexer.class.getResourceAsStream("/models/en-ner-organization.bin");

                if (personModelStream == null || locationModelStream == null|| organizationModelStream == null) {
                    throw new IOException("NER models not found in resources.");
                }

                TokenNameFinderModel personModel = new TokenNameFinderModel(personModelStream);
                TokenNameFinderModel locationModel = new TokenNameFinderModel(locationModelStream);
                TokenNameFinderModel organizationModel = new TokenNameFinderModel(organizationModelStream);

                personNameFinder = new NameFinderME(personModel);
                locationNameFinder = new NameFinderME(locationModel);
                organizationNameFinder = new NameFinderME(organizationModel);

                personModelStream.close();
                locationModelStream.close();
                organizationModelStream.close();
            } else {
                log.info("NER models not enabled (ENABLE_ENTITY = false)");
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
                                    Map<String, Map<String, WordStats>> entityMap,
                                    KVS kvs) throws InterruptedException{
        pageCount++;
        pageDetails.add(page.key()+"\n");
//        System.out.println("processing: " + page.key());

        String url = page.get(CRAWL_URL);
        String text = page.get(CRAWL_TEXT);
        //String ip = page.get(CRAWL_IP);
        String images = page.get(CRAWL_IMAGES);

        // skip op-heavy nlp tasks if url DNE
        if (url == null || url.isEmpty()) {
            System.out.println("Skipping row with no URL: " + page.key());
            return;
        }

        // get urlId base on url
        String urlId;
        try {
            urlId = getUrlId(kvs, url);
        } catch (IOException e) {
            System.err.println("Error fetching/assigning ID for URL: " + url + ", " + e.getMessage());
            return;
        }

        // URL_ID map does not have url mapped
        if (urlId==null) {
            System.out.println("Skipping row with no url id mapped: " + page.key());
            return;
        }

        String[] tokens = tokenizeText(text);

        // parse single normalized words (lemmatize + stop word rm) and populate wordMap
        long startTime = System.currentTimeMillis();
        String[] words = normalizeWord(tokens);
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

        // parse named entities
        if(ENABLE_PARSE_ENTITY){
            extractEntities(tokens, urlId, entityMap); // raw text
        }
    }

    private static void extractEntities(String[] tokens, String urlId, Map<String, Map<String, WordStats>> entityMap) {
        // helper to extract entity in a page

        if (!ENABLE_PARSE_ENTITY || tokens==null) {
            return; // skip ner processing
        }

//        // parse person entities
//        if (personNameFinder != null) {
//            Span[] personSpans = personNameFinder.find(tokens);
//            updateEntityMap(personSpans, tokens, urlId, entityMap);
//        }
//
//        // parse location entities
//        if (locationNameFinder != null) {
//            Span[] locationSpans = locationNameFinder.find(tokens);
//            updateEntityMap(locationSpans, tokens, urlId, entityMap);
//        }
//
//        // parse organization entities
//        if (organizationNameFinder != null) {
//            Span[] organizationSpans = organizationNameFinder.find(tokens);
//            updateEntityMap(organizationSpans, tokens, urlId, entityMap);
//        }

        // parallel processing mult model
        List<Span[]> entitySpans = Arrays.asList(
                personNameFinder.find(tokens),
                locationNameFinder.find(tokens),
                organizationNameFinder.find(tokens)
        );
        entitySpans.parallelStream().forEach(span -> updateEntityMap(span, tokens, urlId, entityMap));


    }

    private static void updateEntityMap(Span[] spans, String[] tokens, String urlId, Map<String, Map<String, WordStats>> entityMap) {
        // helper to populate entity stats

        for (Span span : spans) {
            // multi-token entities -> phrases
            String entity = String.join(" ", Arrays.copyOfRange(tokens, span.getStart(), span.getEnd())).toLowerCase();

            // filter single-worded/6+ entity
            if (span.length() < 2 || span.length() > 5) {
                continue;
            }

            // update entity index map
            entityMap.putIfAbsent(entity, new ConcurrentHashMap<>());
            Map<String, WordStats> urlStatsMap = entityMap.get(entity);

            if (!urlStatsMap.containsKey(urlId)) {
                WordStats stats = new WordStats();
                stats.frequency = 1;
                stats.firstLocation = span.getStart();
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
        Map<String, Map<String, WordStats>> entityMap= new ConcurrentHashMap<>();

        ForkJoinPool forkJoinPool = new ForkJoinPool(2*Runtime.getRuntime().availableProcessors());

        forkJoinPool.submit(() -> pages.forEachRemaining(page -> {
            try {
                processPage(page, wordMap, imageMap, entityMap, kvs);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        })).join();


        try (BufferedWriter pageWriter = new BufferedWriter(new FileWriter("pages_processed.txt", true))) {
            for (String detail : pageDetails) {
                pageWriter.write(detail);
            }
        } catch (IOException e) {
            System.err.println("Error writing pages to file: " + e.getMessage());
        }

        System.out.println("Total pages processed: " + pageCount);
        System.out.println("size of entityMap: " + entityMap.size());
        for(String entity: entityMap.keySet()) System.out.println("entity: " + entity);


        saveIndexToTable(kvs, wordMap, imageMap);
        if (ENABLE_PARSE_ENTITY)
            saveEntityIndexToTable(kvs, entityMap);
    }

    private static void saveEntityIndexToTable(KVS kvs, Map<String, Map<String, WordStats>> entityMap) {
        // helper to save entity to pt-entity-index [schema: Row Name:entity-name; links: urlID:freq,1st]
        Set<String> mergedEntity = new HashSet<>(entityMap.keySet());

//        for(String entity : mergedEntity) {
        mergedEntity.parallelStream().forEach(entity->{
            Map<String, WordStats> urlStatsMap = entityMap.get(entity);
            String rowKey = entity.trim().replaceAll("\\s+", "-").toLowerCase();

            Row row = null;
            try {
                row = kvs.getRow(INDEX_ENTITY_TABLE, rowKey);
            } catch (IOException e) {
                System.out.println("Error: " + e.getMessage());
            }
            if (row == null) {
                row = new Row(rowKey);
            }


            buildLinkColumn(urlStatsMap , row);

            try {
                kvs.putRow(INDEX_ENTITY_TABLE , row);
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());

            }
        });
//        }
    }

    private static void buildLinkColumn(Map<String, WordStats> urlStatsMap , Row row) {
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
    }


    private static void saveIndexToTable (KVS kvs, 
                                          Map<String, Map<String, WordStats>> wordMap, 
                                          Map<String, StringBuffer> imageMap){
        Set<String> mergedWords = new HashSet<>(wordMap.keySet());
        mergedWords.addAll(imageMap.keySet());

        var count = 1;
        long lastTime = System.nanoTime();
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

            buildLinkColumn(urlStatsMap , row);

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
        // helper to find an url's corresponding urlID

        // use cache
        if(URL_ID_CACHE.containsKey(url)){
            System.out.println("URL_ID_CACHE contains url: " + url);
            return URL_ID_CACHE.get(url);
        }

        // use pt-urltoid
        Row row = kvs.getRow(URL_ID_TABLE, Hasher.hash(url));
        if (row != null) {
            String id = row.get(URL_ID_VALUE);
            URL_ID_CACHE.put(url, id);
            return id;
        }

        // didn't find url id in map
        return null;

    }

    private static String[] tokenizeText (String text){
        // helper to tokenize text for both entity and single word
        if (text == null || text.isEmpty()) {
            return null;
        }
        String[] tokens = tokenizer.tokenize(text);
        if (tokens.length == 0) {
            return null;
        }

        return tokens;
    }

    public static String[] normalizeWord(String[] tokens) {
        if (tokens == null || tokens.length == 0) return new String[0];

        List<String> allValidWords = new ArrayList<>(tokens.length);

        String[] batchTokens = new String[Math.min(1000, tokens.length)];
        int batchSize = 0;

        for (String token : tokens) {
            String cached = wordCache.get(token);
            if (cached != null) {
                if (!cached.isEmpty()) {
                    allValidWords.add(cached);
                }
                continue;
            }

            batchTokens[batchSize++] = token;

            if (batchSize == batchTokens.length) {
                processTokenBatch(batchTokens, batchSize, allValidWords);
                batchSize = 0;
            }
        }

        if (batchSize > 0) {
            processTokenBatch(batchTokens, batchSize, allValidWords);
        }

        return allValidWords.toArray(new String[0]);
    }

    private static void processTokenBatch(String[] batchTokens, int batchSize, List<String> allValidWords) {
        String[] actualBatch = Arrays.copyOf(batchTokens, batchSize);

        String[] tags = posTagger.tag(actualBatch);
        String[] lemmas = lemmatizer.lemmatize(actualBatch, tags);

        for (int i = 0; i < batchSize; i++) {
            String originalToken = actualBatch[i];
            String lemma = lemmas[i].toLowerCase();

            if (!lemma.isEmpty() && isValidWord(lemma, tags[i]) && !stopWords.contains(lemma)) {
                allValidWords.add(lemma);
                wordCache.put(originalToken, lemma);
            } else {
                wordCache.put(originalToken, "");
            }
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

            String[] words = normalizeWord(tokenizeText(alt));

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