package org.noova.crawler;

import org.noova.flame.FlameContext;
import org.noova.flame.FlameRDD;
import org.noova.kvs.Row;
import org.noova.tools.Hasher;
import org.noova.tools.Logger;
import org.noova.tools.URLParser;

import java.io.IOException;
import java.io.Serializable;
import java.lang.ref.SoftReference;
import java.net.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Crawler implements Serializable {

    private static final Logger log = Logger.getLogger(Crawler.class);
    public static final String TABLE_PREFIX = "pt-";
    public static final String CRAWLER_TABLE = TABLE_PREFIX + "crawl";
    private static final String HOSTS_TABLE = TABLE_PREFIX + "hosts";
    private static final String LAST_ACCESS_TABLE = "last-access";
    // this is to reduce the pages that have been accessed
    private static final String TRANSIT_ACCESSED_LINK_TABLE = "accessed";
    private static final long DEFAULT_ACCESS_INTERVAL = 1000;
    private static final long DEFAULT_CRAWL_DELAY_IN_SECOND = 1;
    private static final long LOOP_INTERVAL = 1000;
    private static final String CANONICAL_PAGE_TABLE = TABLE_PREFIX + "canonical";
    public static final String ROBOTS_TXT_PATH = "robots.txt";
    private static final String RULE_DISALLOW = "Disallow";
    private static final String RULE_ALLOW = "Allow";
    private static final String RULE_CRAWL_DELAY = "Crawl-delay";
    private static final boolean ENABLE_LOOP_INTERVAL = true;
    private static final boolean ENABLE_LOCK_ACCESS_RATING = false;
    private static final String CIS_5550_CRAWLER = "cis5550-crawler";
//    private static String seedDomain;

    private static final Map<String, SoftReference<String>> URL_CACHE = new WeakHashMap<>();

    private static final Map<String, SoftReference<String>> ROBOT_CACHE = new WeakHashMap<>();


    public static void run(FlameContext ctx, String[] args) throws Exception {
        System.out.println("Crawler is running");
        if (args == null || args.length < 1) {
            log.error("Usage: Crawler <seed-url>");
            ctx.output("Seed URL is not found");
            return;
        }

        String seedUrl = args[0];

        // limit to seed url's domain for crawling first 200k pages
        final String seedDomain = new URI(seedUrl).getHost();
        System.out.println("seed url init: " + seedUrl);
        System.out.println("seed domain init: " + seedDomain);

        String blacklistTable;

        if (args.length > 1) {
            log.warn("[crawler] find blacklist table...");
            blacklistTable = args[1];
        } else {
            log.info("[crawler] No blacklist table found");
            blacklistTable = null;
        }

        log.info("[crawler] Starting crawler with seed URL: " + seedUrl);

        FlameRDD urlQueue = ctx.parallelize(List.of(seedUrl));

        log.info("[crawler] Starting crawler with seed URL: " + Arrays.toString(args));

        while (urlQueue.count() != 0) {
            urlQueue = urlQueue.flatMap(rawUrl -> {
                return processUrl(ctx, rawUrl, blacklistTable, seedDomain);
            });
            if(ENABLE_LOOP_INTERVAL){
                Thread.sleep(LOOP_INTERVAL);
            }
        }

        log.info("[crawler] Crawler finished");

        ctx.output("OK");

    }

    private static List<String> processUrl(FlameContext ctx, String rawUrl, String blacklistTable, String seedDomain) throws Exception {


        String normalizedUrl = normalizeURL(rawUrl, rawUrl);
        if(normalizedUrl == null){
            log.warn("[crawler] URL " + rawUrl + " is not a valid URL. Skipping.");
            return new ArrayList<>();
        }

        log.warn("[crawler] Processing URL: " + normalizedUrl);
        try {
            if(!checkUrlFormat(normalizedUrl)){
                log.warn("[crawler] URL " + normalizedUrl + " is not a valid URL. Skipping.");
                return new ArrayList<>();
            }

            if (isAccessed(ctx, normalizedUrl)) {
                log.warn("[accessed] URL " + normalizedUrl + " has been processed before. Skipping.");
                return new ArrayList<>();
            }

            if (!checkBlackList(ctx, normalizedUrl, blacklistTable)) {
                log.warn("[crawler] URL " + normalizedUrl + " is blocked by blacklist. Skipping.");
                return new ArrayList<>();
            }

            if (!checkRobotRules(ctx, normalizedUrl)) {
                log.warn("[crawler] URL " + normalizedUrl + " is disallowed by robots.txt. Skipping.");
                return new ArrayList<>();
            }

            String hashedUrl = Hasher.hash(normalizedUrl);
//            if(URL_CACHE.containsKey(hashedUrl)) {
//                log.warn("[crawler] [cache hit] URL " + normalizedUrl + " has been processed before. Skipping.");
//                return new ArrayList<>();
//            }

            URL url = new URI(normalizedUrl).toURL();

            if (url.getHost() == null) {
                log.warn("[crawler] Invalid URL: " + normalizedUrl);
                return new ArrayList<>();
            }

            // skip if not in the same domain as seed
            String urlDomain = url.getHost();
            System.out.println("url domain: "+ urlDomain);
            System.out.println("seed: "+ seedDomain);
            if (!urlDomain.equals(seedDomain)) {
                log.warn("[crawler] URL " + normalizedUrl + " is outside the domain " + seedDomain + ". Skipping.");
                return new ArrayList<>();
            }

//            String protocol = url.getProtocol();
//            // check if the protocol is http or https
//            if (!"http".equalsIgnoreCase(protocol) && !"https".equalsIgnoreCase(protocol)) {
//                log.error("[crawler] Invalid protocol: " + protocol);
//                return new ArrayList<>();
//            }

            // this because anchor extraction can create rows before one link being accessed
            Row row = ctx.getKVS().getRow(CRAWLER_TABLE, hashedUrl);
            if(row == null){
                row = new Row(hashedUrl);
            }
            log.info("[crawler] Row: " + row);
            if (!checkLastAccessTime(ctx, normalizedUrl, url.getHost())) {
                // if the host is accessed too frequently, skip this URL, but still need to put it into the table
                return List.of(normalizedUrl);
            }
            //parseHostRules(ctx, normalizedUrl);
            updateHostLastAccessTime(ctx, url.getHost());

            return requestHead(ctx, normalizedUrl, row, blacklistTable);

        } catch (Exception e) {
            log.error("[crawler] Error while processing URL: " + rawUrl, e);
            // if error occurs, ignore
            // return new ArrayList<>();
            return List.of(normalizedUrl);
        }
    }

    public static String filterNonLanguageCharacters(String text) {
        // 保留 Unicode 语言字符和空格，去除数字、符号和标点
        return text.replaceAll("[^\\p{L}\\s]", "");
    }

    private static boolean checkUrlFormat(String normalizedUrl) {
        String lowerCaseUrl = normalizedUrl.toLowerCase();

        Set<String> invalidSuffixFormats = Set.of(".jpg", ".jpeg", ".gif", ".png", ".txt");
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

    private static List<String> requestGet(FlameContext ctx, String normalizedUrl, Row row, String blacklistTable) throws IOException{
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
        updateHostLastAccessTime(ctx, url.getHost());
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
            String[] normalizedPages = normalizePage(page);
            String normalizedPage = String.join(" ", normalizedPages);

            String hashedPage = Hasher.hash(normalizedPage);

            Row pageRow = ctx.getKVS().getRow(CANONICAL_PAGE_TABLE, hashedPage);


            if (pageRow == null || pageRow.get("canonicalURL") == null || pageRow.get("canonicalURL").equals(normalizedUrl)) {
                log.info("[crawler] Creating new canonical URL: " + normalizedUrl);
                row.put("page", normalizedPage);

                pageRow = new Row(hashedPage);
                pageRow.put("canonicalURL", normalizedUrl);
                pageRow.put("page", normalizedPage);

                log.info("[crawler] kvs addr: " + ctx.getKVS().getCoordinator());

                ctx.getKVS().putRow(CRAWLER_TABLE, row);
                ctx.getKVS().putRow(CANONICAL_PAGE_TABLE, pageRow);
            } else {
                String canonicalURL = pageRow.get("canonicalURL");
                log.warn("[crawler] Page is duplicated with + " + canonicalURL + ". Creating canonical URL: " + normalizedUrl);
                row.put("canonicalURL", pageRow.get("canonicalURL"));
                ctx.getKVS().putRow(CRAWLER_TABLE, row);
            }

            return parsePageLinks(ctx, page, normalizedUrl, blacklistTable);

            //String hashedUrl = Hasher.hash(normalizedUrl);
            //URL_CACHE.put(hashedUrl, new SoftReference<>(hashedUrl));
        }
        return new ArrayList<>();
        //return parsePageLinks(ctx, normalizedUrl, blacklistTable);
    }

    static List<String> parsePageLinks(FlameContext ctx, String page, String normalizedUrl, String blacklistTable) throws IOException {
        List<String> links = new ArrayList<>();

        String hashedUrl = Hasher.hash(normalizedUrl);
        if(ctx.getKVS().existsRow(TRANSIT_ACCESSED_LINK_TABLE, hashedUrl)){
            log.info("[crawler] URL " + normalizedUrl + " has been processed before. Ignore this URL.");
            return links;
        }
        ctx.getKVS().put(TRANSIT_ACCESSED_LINK_TABLE, hashedUrl, "url", normalizedUrl);

        String regex = "<a\\s+[^>]*href=[\"']([^\"']*)[\"'][^>]*>([\\s\\S]*?)<\\/a>";
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(page);


        Map<String, StringBuilder> anchorMap = new HashMap<>();

        while (matcher.find()) {
            String href = matcher.group(1).strip();

            // for EC
            String text = matcher.group(2).strip();

            // filter non-lang char
            text = filterNonLanguageCharacters(text);


            String normalizedLink = normalizeURL(href, normalizedUrl);
            if(normalizedLink == null){
                log.warn("[crawler] URL " + href + " is not a valid URL. Skipping.");
                continue;
            }

            if (!checkRobotRules(ctx, normalizedLink)) {
                log.warn("[crawler] URL " + normalizedLink + " is disallowed by robots.txt. Ignore.");
                continue;
            }

            if (!checkBlackList(ctx, normalizedLink, blacklistTable)) {
                log.warn("[crawler] URL " + normalizedLink + " is blocked by blacklist. Ignore.");
                continue;
            }

            if(!checkUrlFormat(normalizedLink)){
                log.warn("[crawler] URL " + normalizedLink + " is not a valid URL. Skipping.");
                continue;
            }

            log.info("[crawler] add link: " + normalizedLink);

            anchorMap.put(normalizedLink, anchorMap.getOrDefault(normalizedLink, new StringBuilder()).append(text).append("<br>"));

            if(isAccessed(ctx, normalizedLink)){
                log.info("[crawler] URL " + normalizedLink + " is accessed before. Ignore.");
                continue;
            }

            links.add(normalizedLink);
        }

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
        return links;
    }

    private static List<String> requestHead(FlameContext ctx, String normalizedUrl, Row row, String blacklistTable) throws IOException, URISyntaxException {
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
            return requestGet(ctx, normalizedUrl, row, blacklistTable);
        }
    }

    private static String[] normalizePage(String page){
        String noHtml = page.replaceAll("<[^>]*>", " ").strip();


        String noPunctuation = noHtml.replaceAll("[.,:;!?'’\"()\\-\\r\\n\\t]", " ").strip();


        String lowerCase = noPunctuation.toLowerCase().strip();

        String[] words = lowerCase.split(" +");
        return words;
    }

    private static boolean checkLastAccessTime(FlameContext ctx, String normalizedUrl, String hostName) throws IOException {
        log.info("[check access] Checking access interval for host: " + hostName);
        long lastAccessTime = getHostLastAccessTime(ctx, hostName);

        long accessInterval = System.currentTimeMillis() - lastAccessTime;


        if (ENABLE_LOCK_ACCESS_RATING) {
            log.info("[check access] Access interval is locked: " + accessInterval);
            if (accessInterval < DEFAULT_ACCESS_INTERVAL) {
                log.warn("[check access] Host " + hostName + " is being accessed too frequently. Skipping URL: " + normalizedUrl);
                return false;
            }
        } else {
            Row row = ctx.getKVS().getRow(HOSTS_TABLE, hostName);
            if (row == null) {
                return true;
            }
            String crawlDelay = row.get(CIS_5550_CRAWLER + ":" + RULE_CRAWL_DELAY) == null ? row.get("*:" + RULE_CRAWL_DELAY) : row.get(CIS_5550_CRAWLER + ":" + RULE_CRAWL_DELAY);
            if (crawlDelay == null) {
                log.info("[check access] No crawl delay found for host: " + hostName + ". Using default value");
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
                log.warn("[check access] Host " + hostName + " is being accessed too frequently. Skipping URL: " + normalizedUrl);
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
        if(rawUrl.isEmpty()){
            return null;
        }
        if(rawUrl.startsWith("..")){
            rawUrl = rawUrl.replace("..", "");
            rawUrl = baseUrl.substring(0, baseUrl.lastIndexOf("/")) + rawUrl;
        }

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
        return protocol + "://" + host + ":" + port + path;
    }

    private static void updateHostLastAccessTime(FlameContext ctx, String hostName) throws IOException {
        ctx.getKVS().put(HOSTS_TABLE, hostName, LAST_ACCESS_TABLE, String.valueOf(System.currentTimeMillis()));
    }

    private static long getHostLastAccessTime(FlameContext ctx, String hostName) throws IOException {
        byte[] lastAccess = ctx.getKVS().get(HOSTS_TABLE, hostName, LAST_ACCESS_TABLE);
        if(lastAccess == null) {
            return 0;
        }
        return Long.parseLong(new String(lastAccess));
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


    private static void downloadRobotsTxt(FlameContext ctx, String normalizedUrl) {

        String robotsTxtUrl = normalizedUrl.endsWith("/") ? normalizedUrl + ROBOTS_TXT_PATH : normalizedUrl + "/" + ROBOTS_TXT_PATH;

//        if(ROBOT_CACHE.containsKey(robotsTxtUrl)) {
//            //log.info("[robot] [cache hit] robots.txt already fetched for: " +  normalizedUrl);
//            return;
//        }


        URL url;
        try {
            url = new URI(robotsTxtUrl).toURL();
        } catch (Exception e) {
            log.error("[robot] Error while parsing URL: " + robotsTxtUrl, e);
            throw new RuntimeException(e);

        }

        try {

            Row row = ctx.getKVS().getRow(HOSTS_TABLE, url.getHost());
            if(row != null && row.get(ROBOTS_TXT_PATH) != null){
                log.info("[robot] robots.txt already fetched for: " +  normalizedUrl);
                return;
            }

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("User-Agent", CIS_5550_CRAWLER);
            conn.connect();
            int responseCode = conn.getResponseCode();
            if(responseCode == 200) {
                String robotsTxt = new String(conn.getInputStream().readAllBytes());
                log.warn("[robot] Found robots.txt: " + robotsTxt);
                ctx.getKVS().put(HOSTS_TABLE, url.getHost(), ROBOTS_TXT_PATH, robotsTxt);
            } else{
                log.warn("[robot] No robots.txt found for: " +  robotsTxtUrl);
                ctx.getKVS().put(HOSTS_TABLE, url.getHost(), ROBOTS_TXT_PATH, "Robot.txt not found");
            }

        } catch(Exception e) {
            log.error("[robot] Error while fetching robots.txt: " + robotsTxtUrl, e);
            try {
                ctx.getKVS().put(HOSTS_TABLE, url.getHost(), ROBOTS_TXT_PATH, "Error while fetching robots.txt");
            } catch (IOException ex) {
                log.error("[robot] Error while saving error message: " + robotsTxtUrl, ex);
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
            // check if the host is in the table
            Row row = ctx.getKVS().getRow(HOSTS_TABLE, url.getHost());
            if (row == null) {
                // if the host is not in the table, download the robots.txt
                downloadRobotsTxt(ctx, normalizedUrl);
                // parse the robots.txt
                parseHostRules(ctx, normalizedUrl);
            }

            row = ctx.getKVS().getRow(HOSTS_TABLE, url.getHost());
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
        String regex = pattern.replace(".", "\\.").replace("*", ".*");
        regex = "^" + regex + ".*";
        return normalizedUrlPath.matches(regex);
    }

}
