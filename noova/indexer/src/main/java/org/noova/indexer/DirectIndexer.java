package org.noova.indexer;

import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;
import org.noova.kvs.KVS;
import org.noova.kvs.KVSClient;
import org.noova.kvs.Row;
import org.noova.tools.*;

import java.io.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.time.LocalDateTime;


public class DirectIndexer {

    private static final Logger log = Logger.getLogger(DirectIndexer.class);
    private static final boolean ENABLE_PARSE_ENTITY = false;
    private static final String DELIMITER = PropertyLoader.getProperty("delimiter.default");
    private static final String INDEX_TABLE = PropertyLoader.getProperty("table.index");
    private static final String INDEX_ENTITY_TABLE = PropertyLoader.getProperty("table.index.entity");
    private static final String INDEX_LINKS = PropertyLoader.getProperty("table.index.links");
    private static final String PROCESSED_URL = PropertyLoader.getProperty("table.processed.url");
    private static final String PROCESSED_IMAGES = PropertyLoader.getProperty("table.processed.images");
    private static final String PROCESSED_TABLE = PropertyLoader.getProperty("table.processed");
    private static final String INDEX_IMAGES = PropertyLoader.getProperty("table.index.images");
    private static final String URL_ID_TABLE = PropertyLoader.getProperty("table.url-id");
    private static final String URL_ID_VALUE = PropertyLoader.getProperty("table.url-id.id");
    private static final String INDEX_IMAGES_TO_PAGE = PropertyLoader.getProperty("table.index.img-page");
    static int pageCount = 0;
    static Queue<String> pageDetails = new ConcurrentLinkedQueue<>();
    static final Map<String, String> URL_ID_CACHE = new WeakHashMap<>();
//    static final String VALID_WORD = "^[a-zA-Z-]+$";

    private static class WordStats {
        int frequency = 0;
        int firstLocation = 0;
    }

    private static TokenizerModel tokenizerModel;
    private static Tokenizer tokenizer;
    private static NameFinderME personNameFinder;
    private static NameFinderME locationNameFinder;
    private static NameFinderME organizationNameFinder;

    static {
        try {
            if (ENABLE_PARSE_ENTITY) {
                InputStream tokenStream = DirectIndexer.class.getResourceAsStream("/models/en-token.bin");
                if (tokenStream == null) {
                    throw new IOException("Tokenizer model not found in resources: /models/en-token.bin");
                }

                log.info("Loading tokenizer model...");
                tokenizerModel = new TokenizerModel(tokenStream);
                tokenizer = new TokenizerME(tokenizerModel);

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

    public static void main(String[] args) throws IOException {
        KVS kvs = new KVSClient(PropertyLoader.getProperty("kvs.host") + ":" + PropertyLoader.getProperty("kvs.port"));
        // load url id to the cache
        System.out.println("Loading URL ID...");
        loadUrlId(kvs);
        System.out.println("URL ID loaded");


        // load url id to the cache
        System.out.println("Loading URL ID...");
        loadUrlId(kvs);
        System.out.println("URL ID loaded");

        for (char c1 = 'a'; c1 <= 'z'; c1++) {
            char c2 = (char) (c1 + 1); // Next character for endKey
            String startKey = String.valueOf(c1);
            String endKey = String.valueOf(c2);
            if (c1 == 'z') {
                endKey = null;
            }
            System.out.println("Processing range: " + startKey + " to " + endKey);

            long start = System.currentTimeMillis();
            System.out.println("Start indexing");
            Iterator<Row> pages = null;
            try {
                pages = kvs.scan(PROCESSED_TABLE, startKey, endKey);
            } catch (IOException e) {
                System.out.println("Error: " + e.getMessage());
                return;
            }

//            Iterator<Row> indexes = null;
//            try {
//                indexes = kvs.scan(INDEX_TABLE, null, null);
//            } catch (IOException e) {
//                System.out.println("Error: " + e.getMessage());
//            }

            generateInvertedIndexBatch(kvs, pages);

            long end = System.currentTimeMillis();

            System.out.println("Time: " + (end - start) + "ms");
        }
    }

    private static void generateInvertedIndexBatch(KVS kvs, Iterator<Row> pages) {
        Map<String, Map<String, WordStats>> wordMap = new ConcurrentHashMap<>();
        Map<String, StringBuffer> imageMap = new ConcurrentHashMap<>();
        Map<String, StringBuffer> imageToPageMap = new ConcurrentHashMap<>();
        Map<String, Map<String, WordStats>> entityMap= new ConcurrentHashMap<>();

        ForkJoinPool forkJoinPool = new ForkJoinPool(2*Runtime.getRuntime().availableProcessors());

        forkJoinPool.submit(() -> pages.forEachRemaining(page -> {
            try {
                processPage(page, wordMap, imageMap, imageToPageMap, entityMap, kvs);
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
//        for(String entity: entityMap.keySet()) System.out.println("entity: " + entity);


        if(imageToPageMap.size() != imageMap.size()){
            System.out.println("Error: imageToPageMap size not equal to imageMap size");
            throw new RuntimeException("Error: imageToPageMap size not equal to imageMap size");
        }

        saveIndexToTable(kvs, wordMap, imageMap, imageToPageMap);
        if (ENABLE_PARSE_ENTITY)
            saveEntityIndexToTable(kvs, entityMap);
    }

    private static void processPage(Row page,
                                    Map<String, Map<String, WordStats>> wordMap,
                                    Map<String, StringBuffer> imageMap,
                                    Map<String, StringBuffer> imageToPageMap,
                                    Map<String, Map<String, WordStats>> entityMap,
                                    KVS kvs) throws InterruptedException{
        pageCount++;
        pageDetails.add(page.key()+"\n");
//        System.out.println("processing: " + page.key());

        String images = page.get(PROCESSED_IMAGES);
        String url = page.get(PROCESSED_URL);
        String rawText = page.get("rawText");  // rawText for entity parsing
        String text = page.get("text");

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

        // populate wordMap for single index using cleanText from pt-processed
        if(text!=null && !text.isEmpty()){
            String[] words = text.split("\\s+");
            for(int i = 0; i < words.length; i++){
                String word = words[i].toLowerCase();
                word = Parser.processWord(word);
                word = Parser.removeAfterFirstPunctuation(word);
                String lemma = LemmaLoader.getLemma(word) != null ? LemmaLoader.getLemma(word) : word;
                System.out.println("Word: " + word+ " lemma: " + lemma);
                if(lemma == null || lemma.isEmpty() || StopWordsLoader.isStopWord(lemma) || !LemmaLoader.getDictionary(lemma)){
                    continue;
                }
                wordMap.putIfAbsent(lemma, new ConcurrentHashMap<>());
                Map<String, WordStats> urlStatsMap = wordMap.get(lemma);

                //System.out.println("Word: " + lemma + " URL: " + urlId);

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

        // parse images and populate imageMap

        if (images != null && !images.isEmpty()) {
            Map<String, Set<String>> wordImageMap = parseImages(images);
            for(Map.Entry<String, Set<String>> entry : wordImageMap.entrySet()){
                String word = entry.getKey();
                Set<String> imageSet = entry.getValue();

                    StringBuffer imageBuffer = imageMap.computeIfAbsent(word, k -> new StringBuffer());
                    StringBuffer imageToPageBuffer = imageToPageMap.computeIfAbsent(word, k -> new StringBuffer());
                    for (String image : imageSet) {
    //                if (!buffer.contains(image)) {

                        if(image.isBlank()){
                            System.out.println("Image is blank or empty for word: " + word);
                            continue;
                        }

                        imageBuffer.append(image).append(DELIMITER);
                        imageToPageBuffer.append(urlId).append(DELIMITER);

                    }
            }
        }

//        System.out.println(rawText);

        // parse named entities
        if(ENABLE_PARSE_ENTITY && rawText != null && !rawText.isEmpty()){
            extractEntities(rawText, urlId, entityMap); // raw text
        }
    }

    private static void extractEntities(String rawText, String urlId, Map<String, Map<String, WordStats>> entityMap) {
        // helper to extract entity in a page

        if (!ENABLE_PARSE_ENTITY || rawText==null || rawText.isEmpty()) {
            return; // skip ner processing
        }

//        System.out.println(rawText);

        // tokenize w/ pos
        Span[] tokenSpans = tokenizer.tokenizePos(rawText);
        String[] tokens = Span.spansToStrings(tokenSpans, rawText);

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
            if (span.length() < 2 || span.length() > 5 || !entity.matches("[a-z ]+")) {
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

//            System.out.println("links: "+ linksBuilder.toString());

            row.put(INDEX_LINKS , linksBuilder.toString());
        }
    }


    private static void saveIndexToTable (KVS kvs,
                                          Map<String, Map<String, WordStats>> wordMap,
                                          Map<String, StringBuffer> imageMap,
                                          Map<String, StringBuffer> imageToPageMap) {
        Set<String> mergedWords = new HashSet<>(wordMap.keySet());
        mergedWords.addAll(imageMap.keySet());

        mergedWords = Collections.synchronizedSet(mergedWords);


        var count = 1;
        long lastTime = System.nanoTime();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

//        for(String word:mergedWords) System.out.println(word+" ");

        for(String word : mergedWords) {
//        mergedWords.parallelStream().forEach(word->{
            if(word.isBlank()) continue;
            Map<String, WordStats> urlStatsMap = wordMap.get(word);
            String images = imageMap.get(word) == null ? "" : imageMap.get(word).toString();
            String imageToPage = imageToPageMap.getOrDefault(word, new StringBuffer()).toString();

//            if(images.isBlank() || imageToPage.isBlank()){
//                continue;
//            }

//            System.out.println("word: "+word);

            Row row = null;
            try {
                row = kvs.getRow(INDEX_TABLE , word);
            } catch (IOException e) {
                System.out.println("Error: " + e.getMessage());
            }

//            System.out.println("row: "+row);

            if (row == null) {
                row = new Row(word);
            }
            if (row.key().isEmpty()) {
                System.out.println("row key " + row.key());
                continue;
            }

            String imgs = row.get(INDEX_IMAGES) == null ? "" : row.get(INDEX_IMAGES);
            String imgToPage = row.get(INDEX_IMAGES_TO_PAGE) == null ? "" : row.get(INDEX_IMAGES_TO_PAGE);
            buildLinkColumn(urlStatsMap , row);



            if(!images.isBlank()){

                imgs = imgs + DELIMITER + images;
            }
            if(!imageToPage.isBlank()) {
                imgToPage = imgToPage + DELIMITER + imageToPage;
            }
            synchronized (DirectIndexer.class) {
                if(!imgs.isBlank()) row.put(INDEX_IMAGES, imgs.trim());
                row.put(INDEX_IMAGES_TO_PAGE, imgToPage);
            }
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

//                System.out.println("putting row: "+row);
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


    static Map<String, Set<String>> parseImages(String images) {
        if(images == null || images.isEmpty()){
            return new ConcurrentHashMap<>();
        }
        Map<String, Set<String>> wordImageMap = new ConcurrentHashMap<>();
        String[] rawImages = images.split("\n");
        for(String rawImage : rawImages){
            String alt = extractImage(rawImage, "alt");
            // only save the src of the image
            String src = extractImage(rawImage, "src");
            if(alt == null || alt.isBlank() || src == null || src.isBlank()){
                continue;
            }

//            String[] words = tokenizeText(alt);
            String[] words = alt.split("\\s+"); // skip nlp processing for img alt

            for(String word : words){
                // ignore empty words
                if(word == null || word.isBlank()){
                    continue;
                }
                word = Parser.processWord(word).toLowerCase();
                word = Parser.removeAfterFirstPunctuation(word);

                String lemma = LemmaLoader.getLemma(word) != null ? LemmaLoader.getLemma(word) : word;
                System.out.println("Word: " + word+ " lemma: " + lemma);
                if (lemma == null || lemma.isEmpty() || StopWordsLoader.isStopWord(lemma) || !LemmaLoader.getDictionary(lemma)) {
                    continue;
                }

                Set<String> imageSet = wordImageMap.computeIfAbsent(lemma, k -> ConcurrentHashMap.newKeySet());
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