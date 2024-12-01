package org.noova.crawler;

import opennlp.tools.lemmatizer.DictionaryLemmatizer;
import opennlp.tools.lemmatizer.Lemmatizer;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import org.noova.kvs.KVS;
import org.noova.kvs.KVSClient;
import org.noova.kvs.Row;
import org.noova.tools.Hasher;
import org.noova.tools.PropertyLoader;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StoreProcessed {
    /*
    Use pt-crawl to populate pt-processed,
    For NER: use parseVisibleText to convert table.crawler.page to table.processed.rawText
    For Single word + preview: use OpenNLP to store fully-processed (lemmatize+stop rm) text to table.processed.cleanText
    */

    private static final String CRAWL_TABLE = PropertyLoader.getProperty("table.crawler");
    private static final String PROCESSED_TABLE = PropertyLoader.getProperty("table.processed");
    private static final String KVS_HOST = PropertyLoader.getProperty("kvs.host");
    private static final String KVS_PORT = PropertyLoader.getProperty("kvs.port");

    private static final TokenizerME tokenizer;
    private static final POSTaggerME posTagger;
    private static final Lemmatizer lemmatizer;
    private static final Set<String> stopWords = new HashSet<>();

    private static final String URL_ID_TABLE = PropertyLoader.getProperty("table.url-id");

    private static final String UNPARSED_LINKS_TABLE = PropertyLoader.getProperty("table.unparsed");

    private static final Map<String, Row> UNPARSED_LINKS_MAP = new HashMap<>();

    static {
        try {
            // load opennlp models
            InputStream tokenStream = StoreProcessed.class.getResourceAsStream("/models/en-token.bin");
            InputStream posStream = StoreProcessed.class.getResourceAsStream("/models/en-pos-maxent.bin");
            InputStream lemmatizerDictStream = StoreProcessed.class.getResourceAsStream("/models/en-lemmatizer.dict.txt");

            if (tokenStream == null || posStream == null || lemmatizerDictStream == null) {
                throw new IOException("Required OpenNLP model files are missing.");
            }

            tokenizer = new TokenizerME(new TokenizerModel(tokenStream));
            posTagger = new POSTaggerME(new POSModel(posStream));
            lemmatizer = new DictionaryLemmatizer(lemmatizerDictStream);

            // load stop words
            InputStream stopWordStream = StoreProcessed.class.getResourceAsStream("/models/stopwords-en.txt");
            if (stopWordStream != null) {
                Scanner scanner = new Scanner(stopWordStream);
                while (scanner.hasNextLine()) {
                    stopWords.add(scanner.nextLine().trim().toLowerCase());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize OpenNLP models: " + e.getMessage(), e);
        }
    }


    public static void main(String[] args) {
        System.out.println("Start processing pt-crawl to pt-processed...");
        KVS kvs = new KVSClient(KVS_HOST + ":" + KVS_PORT);

        int test_run_round = 1;
        // Loop through key pairs (a b, b c, ..., y z)
        for (char c1 = 'a'; c1 <= 'z'; c1++) {
            char c2 = (char) (c1 + 1); // Next character for endKey
            String startKey = String.valueOf(c1);
            String endKey = String.valueOf(c2);
            if (c1 == 'z'){
                endKey = null;
            }
            test_run_round++;
            if (test_run_round<27){
                //continue;
            }
            System.out.println("Processing range: " + startKey + " to " + endKey);

            Iterator<Row> pages = null;
            try {
                pages = kvs.scan(CRAWL_TABLE, startKey, endKey);
            } catch (IOException e) {
                System.err.println("Error scanning range " + startKey + " to " + endKey + ": " + e.getMessage());
                continue;
            }

            Iterator<Row> processedPages = null;
            try {
                processedPages = kvs.scan(PROCESSED_TABLE, null, null);
            } catch (IOException e) {
                System.err.println("Error scanning range " + startKey + " to " + endKey + ": " + e.getMessage());
                continue;
            }

            // Process each slice and populate the new tables
            int count = processSlice(kvs, pages, processedPages);
            System.out.println("Rows converted from pt-crawl to pt-processed: "+count);
        }

        try {
            putUnparsedLinksBatch(kvs);
        } catch (IOException e) {
            System.err.println("Error storing unparsed links: " + e.getMessage());
        }
    }

    private static void putUnparsedLinksBatch(KVS kvs) throws IOException {
        for (Map.Entry<String, Row> entry : UNPARSED_LINKS_MAP.entrySet()) {
            kvs.putRow(UNPARSED_LINKS_TABLE, entry.getValue());
        }
    }


    private static int processSlice(KVS kvs, Iterator<Row> pages, Iterator<Row> processedPages) {
        System.out.println("[processSlice] Processing rows...");
        HashSet <String> processed_page_table = new HashSet<>();
        int counter = 0;
        // dedup
        while (processedPages !=null && processedPages.hasNext()) {
            Row mappingRow = processedPages.next();
            String key = mappingRow.key();
            processed_page_table.add(key);
            counter++;
        }
        System.out.println("[processSlice] Finished create mapping map: " + counter);

        // read crawl pages and store to pt-processed
        while (pages != null && pages.hasNext()) {
            Row page = pages.next();
            String rowKey = page.key();

            if (processed_page_table.contains(rowKey)) {
                System.out.println("Skipping already processed row: " + rowKey);
                continue;
            }

            String rawPageContent = page.get("page");

            if (rawPageContent == null || rawPageContent.isEmpty()) {
                System.out.println("Skipping row with no page content: " + rowKey);
                continue;
            }

            try {
//                System.out.println("Processing row: " + rowKey);
//                System.out.println("Raw data: " + page.toString());

                // use 'page' in pt-crawl to populate 'rawText' in pt-processed
                Element body = Jsoup.parse(rawPageContent).body();
                body.select("script, style, .popup, .ad, .banner, [role=dialog], footer, nav, aside, .sponsored, " +
                        ".advertisement, iframe, span[data-icid=body-top-marquee], div[class^=ad-]").remove();
                String rawText = parseVisibleText(body);

                // use nlp model to generate fully-cleaned text (lemmatize+stop word rm)
                String cleanText = generateCleanText(rawText);
//                String imageText = processImagesForCleanText(page.get("images"));

                // store row for pt-processed
                Row processedRow = new Row(rowKey);
                processedRow.put("rawText", rawText); // for entity index
                processedRow.put("cleanText", cleanText); // for single word index
//                processedRow.put("imageText", imageText); // NEW FIELD - for img index
                if(page.get("url")!=null) processedRow.put("url", page.get("url"));
                if(page.get("ip")!=null) processedRow.put("ip", page.get("ip"));
                if(page.get("title")!=null) processedRow.put("title", page.get("title"));
                if(page.get("images")!=null) processedRow.put("images", page.get("images"));
                if(page.get("timestamp")!=null) processedRow.put("timestamp", page.get("timestamp"));
                if(page.get("description")!=null) processedRow.put("description", page.get("description"));
                if(page.get("addresses")!=null) processedRow.put("addresses", page.get("addresses"));
                if(page.get("icon")!=null) processedRow.put("icon", page.get("icon"));
                //if(page.get("links")!=null) processedRow.put("links", page.get("links")); //! lots of null - need to fix crawler link extraction logic

                // retrieve links from rawPageContent
                String baseUrl = page.get("url");
                if(baseUrl != null){
                    Set<String> links = ParseLinks.parsePageLinks(rawPageContent, baseUrl);
                    if(!links.isEmpty()){
                        processedRow.put("links", String.join("\n", links));
                        // check weather link is already in the table
                        for(String link : links) {
                            String hashedLink = Hasher.hash(link);
                            boolean exist = kvs.existsRow(URL_ID_TABLE, hashedLink);
                            if (!exist) {
                                // store new link
                                Row row = new Row(hashedLink);
                                row.put(PropertyLoader.getProperty("table.default.value"), link);
                                UNPARSED_LINKS_MAP.put(link, row);
                            }
                        }

                    }
                }

                kvs.putRow(PROCESSED_TABLE, processedRow);
                System.out.println("Processed row: " + rowKey);

            } catch (Exception e) {
                System.err.println("Error processing row " + rowKey + ": " + e.getMessage());
                e.printStackTrace();
            }
        }

        System.out.println("[processSlice] Finished processing rows: " + counter);
        return counter;
    }

    private static String parseVisibleText(Element element) {
        StringBuilder textBuilder = new StringBuilder();

        for (Element child : element.children()) {
            String tagName = child.tagName();
            if (tagName.equals("script") || tagName.equals("style")) {
                continue; // Skip non-visible content
            }
            if (child.children().isEmpty()) {
                String text = cleanText(child.text()); // use text() for punc; use ownText() for no punc
                if (!text.isEmpty()) {
                    textBuilder.append(text).append("\n");
                }
            } else {
                textBuilder.append(parseVisibleText(child)).append(" ");
            }
        }

        return textBuilder.toString().trim();
    }

    private static String cleanText(String text) {
        text = text.replaceAll("\\[\\s*\\]", " ");
        text = text.replaceAll("\\s{2,}", " ").trim();
        if (text.length() < 3) {
            return "";
        }
        return text;
    }

    private static String generateCleanText(String rawText) {
        if (rawText == null || rawText.isEmpty()) return "";

        String[] tokens = tokenizer.tokenize(rawText);
        String[] posTags = posTagger.tag(tokens);
        String[] lemmas = lemmatizer.lemmatize(tokens, posTags);

        StringBuilder cleanTextBuilder = new StringBuilder();
        for (int i = 0; i < lemmas.length; i++) {
            String lemma = lemmas[i].toLowerCase();
            if (!stopWords.contains(lemma) && lemma.matches("[a-zA-Z]+")) {
                cleanTextBuilder.append(lemma).append(" "); // cleaned single word separated by space
            }
        }
        return cleanTextBuilder.toString().trim();
    }

//    static String extractImage(String html, String tag) {
//        if(html == null || html.isEmpty()){
//            return "";
//        }
//        String regex = tag + "\\s*=\\s*\"([^\"]*)\"";
//        Pattern pattern = Pattern.compile(regex);
//        Matcher matcher = pattern.matcher(html);
//
//        if (matcher.find()) {
//            // return alt
//            return matcher.group(1);
//        }
//        return "";
//    }
//
//    private static String processImagesForCleanText(String images) {
//        if (images == null || images.isEmpty()) {
//            return "";
//        }
//
//        StringBuilder imageTextBuilder = new StringBuilder();
//        String[] rawImages = images.split("\n");
//
//        for (String rawImage : rawImages) {
//            // Extract 'alt' attribute
//            String altText = extractImage(rawImage, "alt");
//            if (altText == null || altText.isEmpty()) {
//                continue;
//            }
//
//            // nlp process 'alt' text (tokenize, lemmatize, stopword removal)
//            String[] tokens = tokenizer.tokenize(altText);
//            String[] posTags = posTagger.tag(tokens);
//            String[] lemmas = lemmatizer.lemmatize(tokens, posTags);
//
//            for (int i = 0; i < lemmas.length; i++) {
//                String lemma = lemmas[i].toLowerCase();
//                if (!stopWords.contains(lemma) && lemma.matches("[a-zA-Z]+")) {
//                    imageTextBuilder.append(lemma).append(" ");
//                }
//            }
//        }
//
//        return imageTextBuilder.toString().trim();
//    }


}
