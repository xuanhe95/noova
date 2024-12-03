package org.noova.kvs;

import org.noova.tools.Hasher;
import org.noova.tools.PropertyLoader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class KVSUrlCache {
    static final Map<String, String> URL_ID_CACHE = new HashMap<>();

    static final Map<String, String> ID_URL_CACHE = new HashMap<>();

    private static final String URL_ID_TABLE = PropertyLoader.getProperty("table.url-id");
    private static final String URL_ID_VALUE = PropertyLoader.getProperty("table.url-id.id");

    private static final String ID_URL_TABLE = PropertyLoader.getProperty("table.id-url");

    private static final String ID_URL_VALUE = PropertyLoader.getProperty("table.id-url.url");



    private static final KVS KVS_CLIENT = new KVSClient(PropertyLoader.getProperty("kvs.host") + ":" + PropertyLoader.getProperty("kvs.port"));
    private static final String IMG_MAPPING_TABLE = PropertyLoader.getProperty("table.image-mapping");
    //private static KVSUrlCache instance = null;


//    private KVSUrlCache() throws IOException {
//        //this.KVS_CLIENT = kvs;
//        loadUrlId();
//    }
//
//    public static KVSUrlCache getInstance() throws IOException {
//        if (instance == null) {
//            instance = new KVSUrlCache();
//        }
//        return instance;
//    }

    public static String[] hashedImagesToHtml(String hashedImages) {
        String delimiter = PropertyLoader.getProperty("delimiter.default");


        String[] imageArray = hashedImages.split(delimiter);
        String[] htmlArray = new String[imageArray.length];
        for(int i = 0; i < imageArray.length; i++){
            System.out.println("image: " + imageArray[i]);

            Row row = null;
            try {
                row = KVS_CLIENT.getRow(IMG_MAPPING_TABLE, imageArray[i]);
            } catch (IOException e) {
                continue;
            }
            htmlArray[i] = "<img src=\"" + row.get(PropertyLoader.getProperty("table.default.value")) + "\" />";
        }
        return htmlArray;
    }

    public static String getUrlId(String url) throws IOException {
        // helper to find an url's corresponding urlID

        // use cache
        if(URL_ID_CACHE.containsKey(url)){
            System.out.println("URL_ID_CACHE contains url: " + url);
            return URL_ID_CACHE.get(url);
        }

        // use pt-urltoid
        Row row = KVS_CLIENT.getRow(URL_ID_TABLE, Hasher.hash(url));
        if (row != null) {
            String id = row.get(URL_ID_VALUE);
            URL_ID_CACHE.put(url, id);
            return id;
        }

        // didn't find url id in map
        return null;

    }


    public static String getHashedUrl(String id) throws IOException {
        if(URL_ID_CACHE.containsKey(id)){
            return URL_ID_CACHE.get(id);
        }


        Row row = KVS_CLIENT.getRow(ID_URL_TABLE, id);


        if (row != null) {
            String url = row.get(ID_URL_VALUE);
            ID_URL_CACHE.put(id, url);
            return url;
        }

        return null;
    }

    public static void loadAllUrlWithId() throws IOException {
        var ids = KVS_CLIENT.scan(URL_ID_TABLE, null, null);
        ids.forEachRemaining(row -> {
            String id = row.get(URL_ID_VALUE);
            if(id == null){
                return;
            }
            ID_URL_CACHE.put(id, row.key());
            URL_ID_CACHE.put(row.key(), id);
        });
    }

    public static Boolean checkUrlId(String url) throws IOException {
        // helper to find an url's corresponding urlID

        // use cache
        if(URL_ID_CACHE.containsKey(url)){
            System.out.println("URL_ID_CACHE contains url: " + url);
            return true;
        }

        // didn't find url id in map
        return false;

    }


}
