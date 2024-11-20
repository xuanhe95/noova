package org.noova.crawler;

import org.jsoup.nodes.Element;
import org.noova.flame.FlameContext;
import org.noova.flame.FlameRDD;
import org.noova.flame.FlameRDDImpl;
import org.noova.kvs.Row;
import org.noova.tools.Hasher;
import org.noova.tools.Logger;
import org.noova.tools.PropertyLoader;
import org.noova.tools.URLParser;

import java.io.IOException;
import java.io.Serializable;
import java.lang.ref.SoftReference;
import java.net.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

public class Crawler implements Serializable {

    private static final Logger log = Logger.getLogger(Crawler.class);
    public static final String TABLE_PREFIX = "pt-";
    public static final String CRAWLER_TABLE = TABLE_PREFIX + "crawl";
    private static final String HOSTS_TABLE = TABLE_PREFIX + "hosts";
    private static final String LAST_ACCESS_TABLE = "last-access";
    // this is to reduce the pages that have been accessed
    private static final String ACCESSED_LINK_TABLE = "accessed";
    private static final long DEFAULT_ACCESS_INTERVAL = 1000;
    private static final long DEFAULT_CRAWL_DELAY_IN_SECOND = 1;
    private static final long LOOP_INTERVAL = 10;
    private static final String CANONICAL_PAGE_TABLE = TABLE_PREFIX + "canonical";
    public static final String ROBOTS_TXT_PATH = PropertyLoader.getProperty("robots.txt.path");
    private static final String RULE_DISALLOW = "Disallow";
    private static final String RULE_ALLOW = "Allow";
    private static final String RULE_CRAWL_DELAY = "Crawl-delay";
    private static final boolean ENABLE_LOOP_INTERVAL = false; //! polite check
    private static final boolean ENABLE_LOCK_ACCESS_RATING = false;
    private static final boolean ENABLE_VERTICAL_CRAWL = true;

    private static final Random RANDOM_GENERATOR = new Random();

    private static final boolean ENABLE_RANDOM_DROP = true;

    private static final double NORMAL_DROP_RATE = 0.3;

    private static final double HIGH_DROP_RATE = 0.8;

    private static final double LOW_DROP_RATE = 0.1;


    //private static final Set<String> VERTICAL_SEED_DOMAINS = new ConcurrentSkipListSet<>();



    private static final String CIS_5550_CRAWLER = "cis5550-crawler";

    private static final Map<String, SoftReference<String>> URL_CACHE = new WeakHashMap<>();
    private static final Map<String, SoftReference<String>> ROBOT_CACHE = new WeakHashMap<>();
    private static final Map<String, Boolean> BLACKLIST = new ConcurrentHashMap<>();
    private static final boolean ENABLE_ANCHOR_EXTRACTION = false;
    private static final boolean ENABLE_BLACKLIST = false;
    private static final boolean ENABLE_CANONICAL = false;
    private static final boolean ENABLE_ONLY_CIS_5550_ROBOTS = true;
    private static final int LINK_DROP_LENGTH = 128;


    public static void run(FlameContext ctx, String[] args) throws Exception {
        System.out.println("Crawler is running");
        long startTime = System.nanoTime();

        try{
            // each worker work on separate ranges in parallel, e.g., on separate cores.
            int concurrencyLevel = ctx.calculateConcurrencyLevel();
            ctx.setConcurrencyLevel(concurrencyLevel);
            log.info("[crawler] Concurrency level set to: " + concurrencyLevel);

            if (args == null || args.length < 1) {
                log.error("Usage: Crawler <seed-url>");
                ctx.output("Seed URL is not found");
                return;
            }

            // limit to seed urls' domain for crawling first 200k pages
            List<String> seedUrls = List.of(args);
            Set<String> verticalSeedDomains = new HashSet<>();
            for(String url : seedUrls){
                log.info("[crawler] seed url: "+ url);
                String domain = new URI(url).getHost();
                verticalSeedDomains.add(getTopLevelDomain(null, domain));
            }

            log.info("[crawler] seed url init: "+ seedUrls);
            log.info("[crawler] seed domain init: "+ verticalSeedDomains);

            String blacklistTable=null;
            if(ENABLE_BLACKLIST) {
                if (args.length > 1) {
                    log.warn("[crawler] find blacklist table...");
                    blacklistTable = args[1];
                } else {
                    log.info("[crawler] No blacklist table found");
                }
            }

            log.info("[crawler] Starting crawler with seed URL before parallelize: " + seedUrls);

            // adding checkpoints
            FlameRDD urlQueue;
            Iterator<Row> checkpointIterator = ctx.getKVS().scan("pt-checkpoint");

            // loading checkpoint
            if (checkpointIterator.hasNext()) {
                log.info("[crawler] Resuming from checkpoint: pt-checkpoint");

                List<String> checkpointUrls = new ArrayList<>();
                checkpointIterator.forEachRemaining(row -> {
                    String url = row.get("value");
                    if (url != null && !url.isEmpty()) {
                        checkpointUrls.add(url);
                        log.info("[checkpoint] Loaded url: " + url);
                    }
                });

                urlQueue = checkpointUrls.isEmpty() ? ctx.parallelize(seedUrls) : ctx.parallelize(checkpointUrls);
            } else { // start with seed URLs if no checkpoint exists
                log.info("[crawler] No checkpoint found. Starting with seed URLs.");
                urlQueue = ctx.parallelize(seedUrls);
            }

            log.info("[crawler] Starting crawler with seed URL: " + Arrays.toString(args));
            log.info("[crawler] urlQueue count: " + urlQueue.count());

            // save checkpoint on Ctrl+C
            FlameRDD finalUrlQueue = urlQueue;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    log.info("[crawler] Shutdown detected. Saving checkpoint...");
                    ctx.getKVS().delete("pt-checkpoint");
                    finalUrlQueue.saveAsTable("pt-checkpoint");

                    long elapsedTime = System.nanoTime()-startTime;
                    ctx.output("Total time elapsed before shutdown: " +formatElapsedTime(elapsedTime)+"\n");
                    log.info("[crawler] Checkpoint saved successfully during shutdown.");
                } catch (Exception e) {
                    log.error("[crawler] Error saving checkpoint during shutdown.", e);
                }
            }));

            String prevJobTable = null; // placeholder for del old job table

            // main crawling loop
            while (urlQueue.count() > 0) {

                urlQueue = urlQueue.flatMapParallel(rawUrl -> {
                    try {
                        return processUrl(ctx, rawUrl, blacklistTable, verticalSeedDomains);
                    } catch (Exception e) {
                        log.error("Error processing URL: " + rawUrl, e);
                        return List.of();
                    }
                });

                String nextJobTable = ((FlameRDDImpl) urlQueue).getId(); // get next job table ID
                log.info("[crawler] Next Job table ID (before checkpoint): " + nextJobTable);

                // del prev job table
//                if (prevJobTable != null && prevJobTable.startsWith("pt-job-") && !prevJobTable.startsWith("pt-job-0") ) {
//                    log.info("[cleanup] Deleting unused table: " + prevJobTable);
//                    //ctx.getKVS().delete(prevJobTable);
//                }

                // checkpoint the current queue
                log.info("[crawler] Saving checkpoint for URL queue...");
                //ctx.getKVS().delete("pt-checkpoint"); // del previous checkpoint
                //urlQueue.saveAsTable("pt-checkpoint"); // save current queue as checkpoint

                // rotate
                prevJobTable = nextJobTable;

                if(ENABLE_LOOP_INTERVAL){
                    Thread.sleep(LOOP_INTERVAL);
                }
            }

            log.info("[crawler] Crawler finished");

            long elapsedTime = System.nanoTime()-startTime;
            ctx.output("Total time elapsed: " + formatElapsedTime(elapsedTime)+"\n");
            ctx.output("OK");
        } catch (Exception e){
            log.error("[crawler] An error occurred", e);
            long elapsedTime = System.nanoTime() - startTime;
            ctx.output("Total time elapsed before crash: " + formatElapsedTime(elapsedTime)+"\n");
            throw e;
        }
    }

    private static List<String> processUrl(FlameContext ctx, String rawUrl, String blacklistTable, Set<String> verticalSeedDomains) throws Exception {
        String normalizedUrl = normalizeURL(rawUrl, rawUrl);
        if(normalizedUrl == null){
            log.warn("[crawler] URL " + rawUrl + " is not a valid URL. Skipping.");
            return new ArrayList<>();
        }

        log.warn("[crawler] Processing URL: " + normalizedUrl);
        try {
            // filter for dup url
            if (isAccessed(ctx, normalizedUrl)) {
                log.warn("[accessed] URL " + normalizedUrl + " has been processed before. Skipping.");
                return new ArrayList<>();
            }

            // filter for invalid url
            if(!checkUrlFormat(normalizedUrl)){
                log.warn("[crawler] URL " + normalizedUrl + " is not a valid URL. Skipping.");
                return new ArrayList<>();
            }

            // filter for domain name
            URL url = new URI(normalizedUrl).toURL();
            if (url.getHost() == null||url.getPort() < -1 || url.getPort() > 65535) {
                log.warn("[crawler] Invalid host or port in URL: " + normalizedUrl);
                return new ArrayList<>();
            }
            // skip if not in the same domain as seed url
            String topLevelDomain = getTopLevelDomain(null, url.getHost());



            log.info("[crawler] url domain: "+topLevelDomain);

            if (ENABLE_VERTICAL_CRAWL && !verticalSeedDomains.contains(topLevelDomain)) {

                log.warn("[crawler] URL " + normalizedUrl + " is outside the domain " + verticalSeedDomains + ". Skipping.");
                return new ArrayList<>();
            }

            // filter for blacklisted url
            if (ENABLE_BLACKLIST && !checkBlackList(ctx, normalizedUrl, blacklistTable)) {
                log.warn("[crawler] URL " + normalizedUrl + " is blocked by blacklist. Skipping.");
                return new ArrayList<>();
            }

            // filter based on disallow
            if (!checkRobotRules(ctx, normalizedUrl)) {
                log.warn("[crawler] URL " + normalizedUrl + " is disallowed by robots.txt. Skipping.");
                return new ArrayList<>();
            }

            String hashedUrl = Hasher.hash(normalizedUrl);
//            if(URL_CACHE.containsKey(hashedUrl)) {
//                log.warn("[crawler] [cache hit] URL " + normalizedUrl + " has been processed before. Skipping.");
//                return new ArrayList<>();
//            }


//            String protocol = url.getProtocol();
//            // check if the protocol is http or https
//            if (!"http".equalsIgnoreCase(protocol) && !"https".equalsIgnoreCase(protocol)) {
//                log.error("[crawler] Invalid protocol: " + protocol);
//                return new ArrayList<>();
//            }

            // this because anchor extraction can create rows before one link being accessed

            String protocol = url.getProtocol();
            Row row = ctx.getKVS().getRow(CRAWLER_TABLE, hashedUrl);
            if(row == null){
                row = new Row(hashedUrl);
            }
            log.info("[crawler] Row: " + row);
            if (!checkLastAccessTime(ctx, normalizedUrl, getTopLevelDomain(url.getProtocol(), url.getHost()))) {
                // if the host is accessed too frequently, skip this URL, but still need to put it into the table
                return List.of(normalizedUrl);
            }
            //parseHostRules(ctx, normalizedUrl);
            updateHostLastAccessTime(ctx, getTopLevelDomain(url.getProtocol(), url.getHost()));

            return requestHead(ctx, normalizedUrl, row, blacklistTable, verticalSeedDomains);

        } catch (Exception e) {
            log.error("[crawler] Error while processing URL: " + rawUrl, e);
            // if error occurs, ignore
            // return new ArrayList<>();
            return List.of(normalizedUrl);
        }
    }

    public static String filterNonLanguageCharacters(String text) {
        // 保留 Unicode 语言字符和空格，去除数字、符号和标点
        return text.replaceAll("[^\\p{L}\\s]", " ");
    }

    private static boolean checkUrlFormat(String normalizedUrl) {
        String lowerCaseUrl = normalizedUrl.toLowerCase();

        Set<String> invalidSuffixFormats = Set.of(".jpg", ".jpeg", ".gif", ".png", ".txt", ".webp", ".svg");
        Set<String> validPrefixFormats = Set.of("http://", "https://");
        for(String suffix : invalidSuffixFormats){
            if(lowerCaseUrl.endsWith(suffix)){
                log.warn("[crawler] URL " + normalizedUrl + " is an image or text file. Skipping.");
                return false;
            }
        }
        for(String prefix : validPrefixFormats){
            if(lowerCaseUrl.startsWith(prefix)){
                return true;
            }
        }
        return false;
    }

    private static List<String> requestGet(FlameContext ctx, String normalizedUrl, Row row, String blacklistTable, Set<String> verticalSeedDomains) throws IOException{
        URL url;
        try {
            url = new URI(normalizedUrl).toURL();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("User-Agent", CIS_5550_CRAWLER);
        conn.connect();
        updateHostLastAccessTime(ctx, getTopLevelDomain(url.getProtocol(), url.getHost()));
        int responseCode = conn.getResponseCode();



        //        conn = (HttpURLConnection) url.openConnection();
//        conn.setRequestMethod("GET");
//        conn.setRequestProperty("User-Agent", CIS_5550_CRAWLER);
//        conn.connect();
//        responseCode = conn.getResponseCode();
        if (responseCode == 200 && conn.getContentType() != null && conn.getContentType().contains("text/html")) {

            if(isAccessed(ctx, normalizedUrl)){
                log.info("[crawler] URL " + normalizedUrl + " is accessed before. Ignore this URL.");
                return new ArrayList<>();
            }


            byte[] rawPage = conn.getInputStream().readAllBytes();

            String page = new String(rawPage);
            //String[] normalizedPages = normalizePage(page);
            //String normalizedPage = String.join(" ", normalizedPages);

            Document doc = Jsoup.parse(page, normalizedUrl);
            // remove script, style, popup, ad, banner, dialog
            doc.select("script, style, .popup, .ad, .banner, [role=dialog]").remove();
            String visibleText = doc.body().text();
          //  Elements linkElements = doc.select("a");
           // Elements imgElements = doc.select("img");
           // Elements addressElements = doc.select(".address, address");

//            for (Element link : linkElements) {
//                String linkHref = link.attr("href");
//                String linkText = link.text();
//                log.info("[crawler] Link: " + linkHref + " Text: " + linkText);
//            }

            String images = parseImages(doc);
            String addresses = parseAddresses(doc);
            String description = parseDescription(doc);
            String icon = parseIcon(doc, normalizedUrl);
            String title = doc.title().toLowerCase();


            //String normalizedPageText = filterPage(page);

            String hashedPage = Hasher.hash(page);

            // put the page into the table
            row.put(PropertyLoader.getProperty("table.crawler.images"), images);
            // scrawled time
            row.put(PropertyLoader.getProperty("table.crawler.timestamp"), String.valueOf(LocalDateTime.now()));
            // put original page to the page column
            row.put(PropertyLoader.getProperty("table.crawler.page"), page);
            // put normalized page to the text column
            row.put(PropertyLoader.getProperty("table.crawler.text"), visibleText);
            ///row.put(PropertyLoader.getProperty("table.crawler.text"), normalizedPageText);
            // parse titles and put them to the title column
            //List<String> titles = parseTitles(page);
            //row.put(PropertyLoader.getProperty("table.crawler.title"), String.join(" ", titles));
            row.put(PropertyLoader.getProperty("table.crawler.title"), title);

            row.put(PropertyLoader.getProperty("table.crawler.addresses"), addresses);

            row.put(PropertyLoader.getProperty("table.crawler.description"), description);

            row.put(PropertyLoader.getProperty("table.crawler.icon"), icon);

            ctx.getKVS().putRow(CRAWLER_TABLE, row);

            if(ENABLE_CANONICAL){
                Row pageRow = ctx.getKVS().getRow(CANONICAL_PAGE_TABLE, hashedPage);
                if (pageRow == null || pageRow.get("canonicalURL") == null || pageRow.get("canonicalURL").equals(normalizedUrl)) {
                    log.info("[crawler] Creating new canonical URL: " + normalizedUrl);

                    pageRow = new Row(hashedPage);
                    pageRow.put("canonicalURL", normalizedUrl);
                    pageRow.put("page", page);


                    log.info("[crawler] kvs addr: " + ctx.getKVS().getCoordinator());

                    ctx.getKVS().putRow(CANONICAL_PAGE_TABLE, pageRow);
                } else {
                    String canonicalURL = pageRow.get("canonicalURL");
                    log.warn("[crawler] Page is duplicated with + " + canonicalURL + ". Creating canonical URL: " + normalizedUrl);
                }
            }

            return parsePageLinks(ctx, page, normalizedUrl, blacklistTable, verticalSeedDomains);

            //String hashedUrl = Hasher.hash(normalizedUrl);
            //URL_CACHE.put(hashedUrl, new SoftReference<>(hashedUrl));
        }
        return new ArrayList<>();
    }

    static String parseIcon(Document doc, String websiteUrl) throws MalformedURLException {
        Element iconLink = doc.select("link[rel~=(?i)^(icon|shortcut icon)$]").first();
        String iconUrl = null;

        if (iconLink != null) {
            String iconHref = iconLink.attr("href");
            URL baseUrl = new URL(websiteUrl);
            iconUrl = new URL(baseUrl, iconHref).toString();
        } else {
            iconUrl = websiteUrl + "/favicon.ico";
        }

        StringBuilder normalizedHtml = new StringBuilder();
        normalizedHtml.append("<img src=\"").append(iconUrl).append("\" alt=\"icon\" title=\"icon\" onerror=\"this.style.display='none';\" />").append("\n");
        return normalizedHtml.toString().toLowerCase();
    }

    static String parseDescription(Document doc){

        Element metaOgDescription = doc.selectFirst("meta[property=og:description]");
        StringBuilder normalizedHtml = new StringBuilder();
        if (metaOgDescription != null) {
            String description = metaOgDescription.attr("content").strip().replace("\n", " ");
            normalizedHtml.append(description).append("\n");
        }
        return normalizedHtml.toString().toLowerCase();
    }


    static String parseAddresses(Document doc){

        Elements addressElements = doc.select(".address, address div.address, span.location, p.contact-info");
        String addressRegex = "\\d+\\s+[A-Za-z]+(?:\\s+[A-Za-z]+)*,\\s+[A-Za-z]+,\\s+[A-Z]{2}\\s+\\d{5}";
        Pattern pattern = Pattern.compile(addressRegex);

        StringBuilder normalizedHtml = new StringBuilder();

        for (Element address : addressElements) {
            String text = address.text().toLowerCase();
            Matcher matcher = pattern.matcher(text);
            while (matcher.find()) {
                String addressText = matcher.group().strip().replace("\n", " ");
                normalizedHtml.append(addressText).append("\n");
            }
        }
        return normalizedHtml.toString();
    }
    static String parseImages(Document doc){
        Elements imgElements = doc.select("img");
        StringBuilder normalizedHtml = new StringBuilder();

        for (Element img : imgElements) {
            String src = img.attr("abs:src").strip();
            String alt = img.attr("alt").strip().replace("\n", " ").toLowerCase();
            String title = img.attr("title").strip().replace("\n", " ").toLowerCase();

            if (alt.isEmpty()) {
                alt = "No description available";
            }
            if (title.isEmpty()) {
                title = "No title available";
            }

            String normalizedImg = String.format("<img src=\"%s\" alt=\"%s\" title=\"%s\" onerror=\"this.style.display='none';\" />", src, alt, title);
            normalizedHtml.append(normalizedImg).append("\n");
        }
        return normalizedHtml.toString();
    }

    static List<String> parsePageLinks(FlameContext ctx, String page, String normalizedUrl, String blacklistTable, Set<String> verticalSeedDomains) throws IOException {
        List<String> links = new ArrayList<>();

        String hashedUrl = Hasher.hash(normalizedUrl);
        if(ctx.getKVS().existsRow(ACCESSED_LINK_TABLE, hashedUrl)){
            log.info("[crawler] URL " + normalizedUrl + " has been processed before. Ignore this URL.");
            return links;
        }
        ctx.getKVS().put(ACCESSED_LINK_TABLE, hashedUrl, "url", normalizedUrl);

        Map<String, StringBuilder> anchorMap = new HashMap<>();

//        String regex = "<a\\s+[^>]*href=[\"']([^\"']*)[\"'][^>]*>([\\s\\S]*?)<\\/a>";
        String regex = "<a\\s+[^>]*href\\s*=\\s*['\"]?([^'\"\\s>]+)['\"\\s>][^>]*>([\\s\\S]*?)</a>";
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(page);

        while (matcher.find()) {
            String href = matcher.group(1).strip();
            String text = matcher.group(2).strip(); // for EC

            if (href.matches(".*[<>\"'{}|^\\[\\]]+.*")) { // skip href with invalid char
                log.warn("[crawler] Invalid href attribute: " + href);
                continue;
            }

            String normalizedLink = normalizeURL(href, normalizedUrl);

            if(normalizedLink == null){
                log.warn("[crawler] URL " + href + " is not a valid URL. Skipping.");
                continue;
            }

            if(shouldDropLink(normalizedLink, verticalSeedDomains)){
                log.warn("[crawler] URL " + normalizedLink + " is dropped. Skipping.");
                continue;
            }

            // filter invalid
            if(!checkUrlFormat(normalizedLink)){ // this should filter out invalid hyperlinks?
                log.warn("[crawler] URL " + normalizedLink + " is not a valid URL. Skipping.");
                continue;
            }

            // filter blacklist
            if (ENABLE_BLACKLIST && !checkBlackList(ctx, normalizedLink, blacklistTable)) {
                log.warn("[crawler] URL " + normalizedLink + " is blocked by blacklist. Ignore.");
                continue;
            }

            // filter robot
            if (!checkRobotRules(ctx, normalizedLink)) {
                log.warn("[crawler] URL " + normalizedLink + " is disallowed by robots.txt. Ignore.");
                continue;
            }

            log.info("[crawler] add link: " + normalizedLink);

            if(ENABLE_ANCHOR_EXTRACTION){
                text = filterPage(text); // html filter
                anchorMap.put(normalizedLink, anchorMap.getOrDefault(normalizedLink, new StringBuilder()).append(text).append("<br>"));
            }


            if(isAccessed(ctx, normalizedLink)){
                log.info("[crawler] URL " + normalizedLink + " is accessed before. Ignore.");
                continue;
            }

            links.add(normalizedLink);
        }

        if(ENABLE_ANCHOR_EXTRACTION){
            anchorMap.forEach((link, anchor) -> {
                try {
                    Row targetRow = ctx.getKVS().getRow(CRAWLER_TABLE, Hasher.hash(link));
                    if(targetRow == null) {
                        targetRow = new Row(Hasher.hash(link));
                    }
                    // String anchorKey = "anchor:" + KeyGenerator.get().substring(0, 5) + "<!--" + normalizedUrl + "-->";
                    String anchorKey = "anchor:" + normalizedUrl;
                    targetRow.put(anchorKey, anchor.toString());
                    ctx.getKVS().putRow(CRAWLER_TABLE, targetRow);
                } catch (IOException e) {
                    log.error("[crawler] Error while adding anchor to the row: " + link, e);
                    // throw new RuntimeException(e);
                }
            });
        }


        return links;
    }

    public static double calculateDropProbability(int depth, int threshold, double k) {
        // Sigmoid formula: P_drop = 1 / (1 + e^(-k * (depth - threshold)))
        return 1 / (1 + Math.exp(-k * (depth - threshold)));
    }


    private static boolean isRandomPathSegment(String segment) {
        return segment.matches("[a-zA-Z0-9]{10,}");
    }

    private static boolean shouldDropByPathDepth(String[] parts) {
        double randomRatio = RANDOM_GENERATOR.nextDouble();
        int depth = parts.length - 1;
        double pathDropRatio = calculateDropProbability(depth, 8, 0.5);
        for (String segment : parts) {
            // if the path segment is a random string and the path is too deep, drop
            if (isRandomPathSegment(segment) && parts.length > 5) {
                log.warn("[crawler] Random path segment detected: " + String.join("/", parts));
                return true;
            }
        }
        // if the path is too deep
        return randomRatio < pathDropRatio;
    }

    private static boolean shouldDropByDomain(String domain) {

        double randomRatio = RANDOM_GENERATOR.nextDouble();
        // drop if the link is not in the vertical domain
        if(domain.endsWith("edu") || domain.endsWith("org") || domain.endsWith("com") || domain.endsWith("net")){
            if(randomRatio < LOW_DROP_RATE){
                log.info("[crawler] URL " + domain + " is dropped by random ratio. Skipping.");
                return true;
            }
        }
        else if(domain.endsWith("gov") || domain.endsWith("tv") || domain.endsWith("io")){
            if(randomRatio < NORMAL_DROP_RATE){
                log.info("[crawler] URL " + domain + " is dropped by random ratio. Skipping.");
                return true;
            }
        }
        else{
            if(randomRatio < HIGH_DROP_RATE){
                log.info("[crawler] URL " + domain + " is dropped by random ratio. Skipping.");
                return true;
            }
        }
        return false;
    }

    private static boolean containsInvalidProtocol(String link) {
        return link.contains("mailto:") || link.contains("javascript:") || link.contains("tel:");
    }

    private static boolean shouldRandomDrop(String normalizedLink, Set<String> verticalSeedDomains) {
        String[] protocolWithHost = normalizedLink.split("//");
        if(protocolWithHost.length < 2){
            return true;
        }
        String[] parts = protocolWithHost[1].split("/");
        String hostWithPort = parts[0];
        String rawHost = hostWithPort.split(":")[0];


        String topLevelDomain = getTopLevelDomain(null, rawHost);
        // if this is a vertical domain, do not drop
        if(ENABLE_VERTICAL_CRAWL && verticalSeedDomains.contains(topLevelDomain)){
            return false;
        }

        if(shouldDropByPathDepth(parts)){
            log.info("[crawler] URL " + normalizedLink + " is dropped by path depth. Skipping.");
            return true;
        }

        if(shouldDropByDomain(rawHost)){
            return true;
        }
        // if pass all the checks, do not drop
        return false;
    }


    private static boolean shouldDropLink(String normalizedLink, Set<String> verticalSeedDomains) {
        normalizedLink = normalizedLink.toLowerCase();

        if(normalizedLink.length() > LINK_DROP_LENGTH){
            log.warn("[crawler] URL " + normalizedLink + " is too long. Skipping.");
            return true;
        }

        if(containsInvalidProtocol(normalizedLink)){
            // this will always drop
            return true;
        }

        if(ENABLE_RANDOM_DROP && shouldRandomDrop(normalizedLink, verticalSeedDomains)){
            return true;
        }

        // if pass all the checks, do not drop
        return false;
    }

    private static List<String> requestHead(FlameContext ctx, String normalizedUrl, Row row, String blacklistTable, Set<String> verticalSeedDomains) throws IOException, URISyntaxException {
        URL url = new URI(normalizedUrl).toURL();
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("HEAD");
        conn.setRequestProperty("User-Agent", CIS_5550_CRAWLER);
        conn.setInstanceFollowRedirects(false);
        conn.connect();
        int responseCode = conn.getResponseCode();

        log.info("[response] Response code: " + responseCode);

        String contentType = conn.getHeaderField("Content-Type");
        String contentLength = conn.getHeaderField("Content-Length");
        row.put("url", normalizedUrl);
        row.put("responseCode", String.valueOf(responseCode));

        if (contentLength != null) {
            row.put("length", contentLength);
        }

        if (contentType != null) {
            row.put("contentType", contentType);
        }

        InetAddress ip = InetAddress.getByName(url.getHost());
        log.info("[crawler] IP: " + ip.getHostAddress());
        row.put("ip", ip.getHostAddress());

        if (responseCode == 301 || responseCode == 302 || responseCode == 303 || responseCode == 307 || responseCode == 308) {
            log.info("[redirect] Redirect " + responseCode + " is detected. URL: " + normalizedUrl);
            String location = conn.getHeaderField("Location");
            ctx.getKVS().putRow(CRAWLER_TABLE, row);
            if (location != null) {
                // redirect to the new location
                location = normalizeURL(location, normalizedUrl);
                if(location == null){
                    log.error("[redirect] Invalid URL: " + location);
                    return new ArrayList<>();
                }
                return List.of(location);
            } else {
                log.error("[redirect] No location found in the response header. URL: " + normalizedUrl);
                return new ArrayList<>();
            }
        } else if (responseCode != 200) {
            log.warn("[response] Error Response code: " + responseCode);
            ctx.getKVS().putRow(CRAWLER_TABLE, row);
            return new ArrayList<>();
        } else {
            return requestGet(ctx, normalizedUrl, row, blacklistTable, verticalSeedDomains);
        }
    }

    public static String filterPage(String page) {
        if(page == null) {
            return "";
        }

        // comprehensive html filter
        // filter script style tag
        page = page.replaceAll("(?s)<script.*?>.*?</script>", " ").strip();
        page = page.replaceAll("(?s)<style.*?>.*?</style>", " ").strip();

        // filter event handler
        page = page.replaceAll("on\\w+\\s*=\\s*\"[^\"]*\"", " ").strip();
        page = page.replaceAll("on\\w+\\s*=\\s*'[^']*'", " ").strip();

        // filter html comments
        page = page.replaceAll("(?s)<!--.*?-->", " ").strip();

        // filter footer tags
        page = page.replaceAll("(?s)<footer.*?>.*?</footer>", " ").strip();

        // filter hidden tags
        page = page.replaceAll("(?s)<(meta|head|noscript|iframe|embed|object|applet|link|base|area|map|param|track|wbr)[^>]*>.*?</\\1>", " ").strip();
        page = page.replaceAll("(?s)<(meta|head|noscript|iframe|embed|object|applet|link|base|area|map|param|track|wbr)[^>]*>", " ").strip();

        // filter div, p, h1-6, br
        page = page.replaceAll("(?i)<(div|p|h[1-6]|br)[^>]*>", "\n").strip();

        // filter remaining html tags
        page = page.replaceAll("<[^>]*>", " ").strip();

        // normalize whitespace
        page = page.replaceAll("\\s+", " ").strip();

        // misc- filter boilerplate content like 'about us'
        page = page.replaceAll("(?i)\\b(privacy policy|terms of service|about us)\\b", " ").strip();

        page = page.replaceAll("[.,:;!?'’\"()\\-\\r\\n\\t]", " ").strip();

        // filter out non-letters
        page = page.replaceAll("[^\\p{L}\\s]", " ").strip();

        page = page.toLowerCase();

        return page;
    }


    public static List<String> parseTitles(String page) {
        if (page == null) {
            return new ArrayList<>();
        }

        List<String> titles = new ArrayList<>();
        Pattern titlePattern = Pattern.compile("(?i)<title[^>]*>(.*?)</title>");
        Matcher matcher = titlePattern.matcher(page);

        while (matcher.find()) {
            // extract the content inside <title> tag
            String title = matcher.group(1).strip();
            titles.add(title);
        }

        return titles;
    }


    private static String[] normalizePage(String page){

        // match <p> and <h1> to <h6> tags

        Pattern pattern = Pattern.compile("(?s)<(p|h[1-6]).*?>(.*?)</\\1>");


        Matcher matcher = pattern.matcher(page);

        StringBuilder htmlContent = new StringBuilder();

        // only keep the content inside <body> tag
        while (matcher.find()) {
            htmlContent.append(matcher.group(2)).append(" ");


        }

        String content = filterNonLanguageCharacters(htmlContent.toString());

        // remove script tags
        content = content.replaceAll("(?s)<script.*?>.*?</script>", " ").strip();

        String noHtml = content.replaceAll("<[^>]*>", " ").strip();


        String noPunctuation = noHtml.replaceAll("[.,:;!?'’\"()\\-\\r\\n\\t]", " ").strip();


        String lowerCase = noPunctuation.toLowerCase().strip();

        String[] words = lowerCase.split(" +");
        return words;
    }

    private static boolean checkLastAccessTime(FlameContext ctx, String normalizedUrl, String topLevelDomain) throws IOException {
        log.info("[check access] Checking access interval for host: " + topLevelDomain);
        long lastAccessTime = getHostLastAccessTime(ctx, topLevelDomain);

        long accessInterval = System.currentTimeMillis() - lastAccessTime;


        if (ENABLE_LOCK_ACCESS_RATING) {
            log.info("[check access] Access interval is locked: " + accessInterval);
            if (accessInterval < DEFAULT_ACCESS_INTERVAL) {
                log.warn("[check access] Host " + topLevelDomain + " is being accessed too frequently. Skipping URL: " + normalizedUrl);
                return false;
            }
        } else {
            String hashedTopLevelDomain = Hasher.hash(topLevelDomain);
            Row row = ctx.getKVS().getRow(HOSTS_TABLE, hashedTopLevelDomain);
            if (row == null) {
                return true;
            }
            String crawlDelay = row.get(CIS_5550_CRAWLER + ":" + RULE_CRAWL_DELAY) == null ? row.get("*:" + RULE_CRAWL_DELAY) : row.get(CIS_5550_CRAWLER + ":" + RULE_CRAWL_DELAY);
            if (crawlDelay == null) {
                log.info("[check access] No crawl delay found for host: " + topLevelDomain + ". Using default value");
                crawlDelay = String.valueOf(DEFAULT_CRAWL_DELAY_IN_SECOND);
            }
            double delayInSecond;
            try {
                log.info("[crawl delay] Crawl delay: " + crawlDelay);
                delayInSecond = Double.parseDouble(crawlDelay);
            } catch (Exception e) {
                log.error("[crawl delay] Error while parsing crawl delay: " + crawlDelay + " using default value");
                delayInSecond = 0;
            }

            if (accessInterval < delayInSecond * 1000) {
                log.warn("[check access] Host " + topLevelDomain + " is being accessed too frequently. Skipping URL: " + normalizedUrl);
                return false;
            }
        }
        return true;
    }

    private static boolean checkBlackList(FlameContext ctx, String normalizedUrl, String blacklistTable) {
        if (blacklistTable == null) {
            return true;
        }
        try {
            Iterator<Row> it = ctx.getKVS().scan(blacklistTable);


            while (it != null && it.hasNext()) {
                Row row = it.next();
                String pattern = row.get("pattern");
                if (isBlocked(normalizedUrl, pattern)) {
                    log.warn("[crawler] URL " + normalizedUrl + " is blocked by blacklist pattern: " + pattern);
                    return false;
                }
            }

            return true;

        } catch (IOException e) {
            log.error("[crawler] Error while checking blacklist", e);
            throw new RuntimeException(e);
        }
    }

    static String normalizeURL(String rawUrl, String baseUrl){
        if(rawUrl.contains("#")){
            rawUrl = rawUrl.substring(0, rawUrl.indexOf("#"));
        }

        rawUrl = rawUrl.trim();
        rawUrl = rawUrl.replaceAll("\\\\+$", "");

        if(rawUrl.isEmpty()){
            return null;
        }

        if (rawUrl.matches(".*[<>\"'{}|^\\[\\]]+.*")) { // invalid char in html detected
            log.warn("[normalizeURL] Invalid characters detected in URL: " + rawUrl);
            return null;
        }
        if(rawUrl.startsWith("..")){
            rawUrl = rawUrl.replace("..", "");
            rawUrl = baseUrl.substring(0, baseUrl.lastIndexOf("/")) + rawUrl;
        }

        try{
            rawUrl = rawUrl.replace(" ", "%20");
            String[] parts = URLParser.parseURL(rawUrl);

            String protocol;
            String host;
            String port;
            String path;

            if (parts[0] == null && parts[1] == null) {
                protocol = URLParser.parseURL(baseUrl)[0] == null ? "http" : URLParser.parseURL(baseUrl)[0];
                host = URLParser.parseURL(baseUrl)[1];
                port = URLParser.parseURL(baseUrl)[2] == null ? "http".equals(protocol) ? "80" : "443" : URLParser.parseURL(baseUrl)[2];
                path = parts[3];
            } else {
                protocol = parts[0] == null ? "http" : parts[0];
                host = parts[1] == null ? URLParser.parseURL(baseUrl)[1] : parts[1];
                port = parts[2] == null ? "http".equals(protocol) ? "80" : "443" : parts[2];
                path = parts[3];
            }
            if (host != null) {
                host = host.replaceAll("[^a-zA-Z0-9.-]", "");
            }
            if(path!=null && !path.startsWith("/")) {
                path = "/"+path;
            }
            if (path != null) {
                path = path.replaceAll("//+", "/");
            }
            return protocol + "://" + host + ":" + port + path;
        } catch(Exception e){
            log.error("[normalizeURL] Malformed URL: " + rawUrl, e);
            return null;
        }

    }

    private static void updateHostLastAccessTime(FlameContext ctx, String hostName) throws IOException {
        String hashedTopLevelDomain = Hasher.hash(hostName);
        ctx.getKVS().put(HOSTS_TABLE, hashedTopLevelDomain, LAST_ACCESS_TABLE, String.valueOf(System.currentTimeMillis()));
        ctx.getKVS().put(HOSTS_TABLE, hashedTopLevelDomain, "url", hostName);
    }

    private static long getHostLastAccessTime(FlameContext ctx, String hostName) throws IOException {
        String hashedTopLevelDomain = Hasher.hash(hostName);
        byte[] lastAccess = ctx.getKVS().get(HOSTS_TABLE, hashedTopLevelDomain, LAST_ACCESS_TABLE);
        if(lastAccess == null) {
            return 0;
        }
        return Long.parseLong(new String(lastAccess));
    }

    private static void efficientParseHostRules(FlameContext ctx, String robotsTxt, Row row){

        try {
            if(robotsTxt == null) {
                return;
            }

            String[] lines = robotsTxt.split("\n");

            String userAgent = null;

            Map<String, Map<String, StringBuilder>> result = new HashMap<>();

            Map<String, StringBuilder> rules = new HashMap<>();
            for(String line : lines) {
                if(line.startsWith("User-agent:")) {
                    //log.info("[crawler] User-agent: " + line);
                    if(userAgent != null) {
                        result.put(userAgent, rules);
                        rules = new HashMap<>();
                    }
                    userAgent = line.split(":")[1].strip();

                } else if(userAgent != null && line.strip().toLowerCase().startsWith(RULE_DISALLOW.toLowerCase())) {
                    /* for case disallow: empty
                    User-agent: Pinterest
                    Disallow:
                     */
                    if (line.split(":").length==1){
                        continue;
                    }
                    String path = line.split(":")[1].strip();

                    StringBuilder rule = rules.getOrDefault(RULE_DISALLOW, new StringBuilder()).append(path).append("\n");
                    rules.put(RULE_DISALLOW, rule);
                } else if(userAgent != null && line.strip().toLowerCase().startsWith(RULE_ALLOW.toLowerCase())) {
                    String path = line.split(":")[1].strip();
                    StringBuilder rule = rules.getOrDefault(RULE_ALLOW, new StringBuilder()).append(path).append("\n");
                    rules.put(RULE_ALLOW, rule);
                } else if(userAgent != null && line.strip().toLowerCase().startsWith(RULE_CRAWL_DELAY.toLowerCase())) {
                    String delay = line.split(":")[1].strip();
                    StringBuilder rule = new StringBuilder(delay);
                    rules.put(RULE_CRAWL_DELAY, rule);
                }
            }

            if(userAgent != null) {
                result.put(userAgent, rules);
            }

            result.forEach((user, rule) -> {
                log.info("[crawler] User-agent: " + user);

                if(ENABLE_ONLY_CIS_5550_ROBOTS) {
                    if(!user.equals(CIS_5550_CRAWLER) && !user.equals("*")) {
                        log.info("[crawler] Skip user-agent: " + user);
                        return;
                    }
                }
                rule.forEach((key, value) -> {
                    log.info("[crawler] Rule: " + key + " -> " + value);
                    row.put(user+":"+key, value.toString());
                });
            });

            ctx.getKVS().putRow(HOSTS_TABLE,row);

        } catch(Exception e) {
            log.error("[crawler] Error while getting robots.txt: " + robotsTxt, e);
            throw new RuntimeException(e);
        }
    }

    private static void parseHostRules(FlameContext ctx, String normalizedUrl){
        String robotsTxtUrl = normalizedUrl.endsWith("/") ? normalizedUrl + ROBOTS_TXT_PATH : normalizedUrl + "/" + ROBOTS_TXT_PATH;

        try {
            URL url = new URI(robotsTxtUrl).toURL();
            Row row = ctx.getKVS().getRow(HOSTS_TABLE, url.getHost());

            if(row == null) {
                return;
            }

            String robotsTxt = row.get(ROBOTS_TXT_PATH);

            if(robotsTxt == null) {
                return;
            }

            String[] lines = robotsTxt.split("\n");

            String userAgent = null;

            Map<String, Map<String, StringBuilder>> result = new HashMap<>();

            Map<String, StringBuilder> rules = new HashMap<>();
            for(String line : lines) {
                if(line.startsWith("User-agent:")) {
                    //log.info("[crawler] User-agent: " + line);
                    if(userAgent != null) {
                        result.put(userAgent, rules);
                        rules = new HashMap<>();
                    }
                    userAgent = line.split(":")[1].strip();

                } else if(userAgent != null && line.strip().toLowerCase().startsWith(RULE_DISALLOW.toLowerCase())) {
                    /* for case disallow: empty
                    User-agent: Pinterest
                    Disallow:
                     */
                    if (line.split(":").length==1){
                        continue;
                    }
                    String path = line.split(":")[1].strip();

                    StringBuilder rule = rules.getOrDefault(RULE_DISALLOW, new StringBuilder()).append(path).append("\n");
                    rules.put(RULE_DISALLOW, rule);
                } else if(userAgent != null && line.strip().toLowerCase().startsWith(RULE_ALLOW.toLowerCase())) {
                    String path = line.split(":")[1].strip();
                    StringBuilder rule = rules.getOrDefault(RULE_ALLOW, new StringBuilder()).append(path).append("\n");
                    rules.put(RULE_ALLOW, rule);
                } else if(userAgent != null && line.strip().toLowerCase().startsWith(RULE_CRAWL_DELAY.toLowerCase())) {
                    String delay = line.split(":")[1].strip();
                    StringBuilder rule = new StringBuilder(delay);
                    rules.put(RULE_CRAWL_DELAY, rule);
                }
            }

            if(userAgent != null) {
                result.put(userAgent, rules);
            }

            result.forEach((user, rule) -> {
                log.info("[crawler] User-agent: " + user);
                rule.forEach((key, value) -> {
                    row.put(user+":"+key, value.toString());
                });
            });

            ctx.getKVS().putRow(HOSTS_TABLE,row);

        } catch(Exception e) {
            log.error("[crawler] Error while getting robots.txt: " + robotsTxtUrl, e);
            throw new RuntimeException(e);
        }
    }


    private static void downloadAndParseRobotsTxt(FlameContext ctx, String topLevelDomain, Row row) {



//        String robotsTxtUrl = normalizedUrl.endsWith("/") ? normalizedUrl + ROBOTS_TXT_PATH : normalizedUrl + "/" + ROBOTS_TXT_PATH;
//
////        if(ROBOT_CACHE.containsKey(robotsTxtUrl)) {
////            //log.info("[robot] [cache hit] robots.txt already fetched for: " +  normalizedUrl);
////            return;
////        }
//
//
//        URL url;
//        try {
//            url = new URI(robotsTxtUrl).toURL();
//        } catch (Exception e) {
//            log.error("[robot] Error while parsing URL: " + robotsTxtUrl, e);
//            throw new RuntimeException(e);
//        }
        if(row == null) {
            log.error("[robot] Row is null for: " + topLevelDomain);
            return;
        }

        if(row.get(ROBOTS_TXT_PATH) != null) {
            log.info("[robot] robots.txt already fetched for: " + topLevelDomain);
            return;
        }

        String robotsTxtUrl = topLevelDomain + "/" + ROBOTS_TXT_PATH;
        HttpURLConnection conn = null;

        try {

            // Row row = ctx.getKVS().getRow(HOSTS_TABLE, getTopLevelDomain(url.getHost()));
//            if(row.get(ROBOTS_TXT_PATH) != null){
//                log.info("[robot] robots.txt already fetched for: " +  topLevelDomain);
//                return;
//            }

//            if(row == null){
//                row = new Row(getTopLevelDomain(url.getHost()));
//            }

            URL robotsUrl = new URI(robotsTxtUrl).toURL();
            conn = (HttpURLConnection) robotsUrl.openConnection();
            // in case of slow response
            conn.setConnectTimeout(500);
            conn.setReadTimeout(500);
            conn.setRequestMethod("GET");
            conn.setRequestProperty("User-Agent", CIS_5550_CRAWLER);
            conn.connect();

            int responseCode = conn.getResponseCode();
            if(responseCode == 200 && conn.getContentType() != null && conn.getContentType().contains("text/plain")) {
                String robotsTxt = new String(conn.getInputStream().readAllBytes());
                log.warn("[robot] Found robots.txt: " + robotsTxt);

                row.put(ROBOTS_TXT_PATH, robotsTxt);
                // ctx.getKVS().put(HOSTS_TABLE, getTopLevelDomain(url.getHost()), ROBOTS_TXT_PATH, robotsTxt);

                efficientParseHostRules(ctx, robotsTxt, row);


            }else {
                row.put(ROBOTS_TXT_PATH, "NOT_FOUND");
            }
        } catch(Exception e) {
            log.error("[robot] Error while fetching robots.txt: " + robotsTxtUrl, e);
            row.put(ROBOTS_TXT_PATH, "ERROR");
        }finally {
            if(conn != null) {
                conn.disconnect();
            }
            try {
                ctx.getKVS().putRow(HOSTS_TABLE, row);
            } catch(Exception e) {
                log.error("[robot] Failed to save row for: " + topLevelDomain);
            }
        }
//        } finally{
//            ROBOT_CACHE.put(robotsTxtUrl, new SoftReference<>(robotsTxtUrl));
//        }
    }

    private static boolean isBlocked(String normalizedUrl, String blacklistPattern){
        if(blacklistPattern == null){
            return false;
        }
        String regex = blacklistPattern.replace(".", "\\.").replace("*", ".*");
        return normalizedUrl.matches(regex);
    }

    private static boolean isAccessed(FlameContext ctx, String normalizedUrl){
        String hashedUrl = Hasher.hash(normalizedUrl);
        try {
            Row row = ctx.getKVS().getRow(CRAWLER_TABLE, hashedUrl);
            if(row == null){
                return false;
            }
            return row.get("url") != null;
            // return ctx.getKVS().existsRow(TRANSIT_ACCESSED_LINK_TABLE, normalizedUrl);
        } catch (IOException e) {
            log.error("[crawler] Error while checking if URL is accessed: " + normalizedUrl, e);
            return false;
            //throw new RuntimeException(e);
        }
    }

    private static boolean checkRobotRules(FlameContext ctx, String normalizedUrl)  {
        try {
            URL url = new URI(normalizedUrl).toURL();
//            // check port
//            int port = url.getPort();
//            if (port != -1 && (port < 1 || port > 65535)) {
//                log.warn("[checkRobotRules] Invalid port in URL: " + normalizedUrl);
//                return false; // allow processing if malformed?
//            }



            String topLevelDomainName = getTopLevelDomain(url.getProtocol(), url.getHost());

            String hashedTopLevelDomain = Hasher.hash(topLevelDomainName);

            // check if the host is in the table
            Row row = ctx.getKVS().getRow(HOSTS_TABLE, hashedTopLevelDomain);
            if (row == null) {
                row = new Row(hashedTopLevelDomain);
                row.put("url", topLevelDomainName);
                // if the host is not in the table, download the robots.txt
                downloadAndParseRobotsTxt(ctx, topLevelDomainName, row);
                // parse the robots.txt
                // parseHostRules(ctx, normalizedUrl);
            }

            // after parse, get again
            row = ctx.getKVS().getRow(HOSTS_TABLE, topLevelDomainName);
            if (row == null) {
                // if the host is still not in the table, return true
                return true;
            }

            String disallow = row.get(CIS_5550_CRAWLER+":"+RULE_DISALLOW) == null ? row.get("*:"+RULE_DISALLOW) : row.get(CIS_5550_CRAWLER+":"+RULE_DISALLOW);
            String allow = row.get(CIS_5550_CRAWLER+":"+RULE_ALLOW) == null ? row.get("*:"+RULE_ALLOW) : row.get(CIS_5550_CRAWLER+":"+RULE_ALLOW);
            String delay = row.get(CIS_5550_CRAWLER+":"+RULE_CRAWL_DELAY) == null ? row.get("*:"+RULE_CRAWL_DELAY) : row.get(CIS_5550_CRAWLER+":"+RULE_CRAWL_DELAY);

            String[] allowedPaths = allow == null ? new String[0] : allow.split("\n");
            String[] disallowedPaths = disallow == null ? new String[0] : disallow.split("\n");

            String allowedNearestPath = null;
            String disallowedNearestPath = null;

            for (String path : allowedPaths) {
                if(startWithPattern(url.getPath(), path)){
                    int distance = url.getPath().length() - path.length();
                    if(allowedNearestPath == null || distance < url.getPath().length() - allowedNearestPath.length()){
                        // if the path is shorter than the previous one, this path is closer to the URL
                        allowedNearestPath = path;
                    }
                }
            }

            for (String path : disallowedPaths) {
                if(startWithPattern(url.getPath(), path)){
                    int distance = url.getPath().length() - path.length();
                    if(disallowedNearestPath == null || distance < url.getPath().length() - disallowedNearestPath.length()){
                        // if the path is shorter than the previous one, this path is closer to the URL
                        disallowedNearestPath = path;
                    }
                }
            }

            if(allowedNearestPath != null && disallowedNearestPath != null){
                // both allowed and disallowed path are found
                if(url.getPath().length() - allowedNearestPath.length() > url.getPath().length() - disallowedNearestPath.length()){
                    // disallowed path is closer
                    return false;
                } else {
                    // allowed path is closer
                    return true;
                }
            } else if(disallowedNearestPath != null){
                // only disallowed path is found
                return false;
            } else{
                // no disallowed path is found
                return true;
            }
        } catch(Exception e){
            log.error("[crawler] Error while checking robot rules: " + normalizedUrl, e);
            return true;
        }
    }

    private static boolean startWithPattern(String normalizedUrlPath, String pattern) {
        try{
//            String regex = pattern.replace(".", "\\.").replace("*", ".*");
            String regex = pattern.replaceAll("([\\\\.*+?\\[^\\]$(){}=!<>|:\\-])", "\\\\$1");
            regex = "^" + regex + ".*";
            return normalizedUrlPath.matches(regex);
        } catch (PatternSyntaxException e) {
            log.error("[crawler] Invalid pattern in robots.txt: " + pattern, e);
            return false;
        }

    }

    // record time for each crawler run
    private static String formatElapsedTime(long nanoTime) {
        long totalSeconds = nanoTime / 1_000_000_000;
        long hours = totalSeconds / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        long seconds = totalSeconds % 60;
        long milliseconds = (nanoTime / 1_000_000) % 1000;

        return String.format("%02d:%02d:%02d.%03d", hours, minutes, seconds, milliseconds);
    }

    private static String getTopLevelDomain(String protocol, String host) {
        String[] parts = host.split("\\.");
        if (parts.length < 2) {
            return host;
        }
        if(protocol == null){
            return parts[parts.length - 2] + "." + parts[parts.length - 1];
        }
        return protocol + "://" + parts[parts.length - 2] + "." + parts[parts.length - 1];
    }

}
