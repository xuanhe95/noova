package org.noova.gateway.service;

import org.noova.gateway.storage.StorageStrategy;
import org.noova.kvs.KVS;
import org.noova.kvs.KVSClient;
import org.noova.kvs.Row;
import org.noova.tools.PropertyLoader;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ImageService implements IService{
    private static final StorageStrategy STORAGE_STRATEGY = StorageStrategy.getInstance();

//    public static byte[] getImage(String key){
//        return storageStrategy.get(key);
//    }

    private static ImageService instance;

    private static final KVS KVS_CLIENT = new KVSClient(PropertyLoader.getProperty("kvs.host") + ":" + PropertyLoader.getProperty("kvs.port"));

    private static final String IMAGE_TABLE = PropertyLoader.getProperty("table.image");

    private static final String IMAGE_MAPPING_TABLE = PropertyLoader.getProperty("table.image-mapping");

    private static Map<String, String> IMAGE_MAP = new HashMap<>();


    private ImageService() {
//        if (ENABLE_TRIE_CACHE) {
//            try {
//                //trie = TrieManager.getInstance().loadTrie(PropertyLoader.getProperty("table.index"));
//
//                String trieName = PropertyLoader.getProperty("table.trie.default");
//
//                if(storageStrategy.containsKey(
//                        PropertyLoader.getProperty("table.trie"),
//                        trieName,
//                        "test"
//                )){
//                    log.info("[search] Trie found");
//                    trie = trieManager.loadTrie(trieName);
//                } else{
//                    log.info("[search] Trie not found, building trie...");
//                    trie = trieManager.buildTrie(PropertyLoader.getProperty("table.index"));
//                    trieManager.saveTrie(trie, trieName);
//                }
//
//            } catch (IOException e) {
//                log.error("[search] Error loading trie");
//            }
//        }

    }

    public static ImageService getInstance() {
        if (instance == null) {
            instance = new ImageService();
        }
        return instance;
    }

    public Map<String, Set<String>> searchByKeyword(String keyword) throws IOException {
        Row row = KVS_CLIENT.getRow(IMAGE_TABLE, keyword);
        if (row == null) {
            return new HashMap<>();
        }

        Map<String, Set<String>> result = new HashMap<>();
        Set<String> fromIds = row.columns();

        for (String fromUrlId : fromIds) {
            String images = row.get(fromUrlId);
            String[] hashedImages = images.split("\n");

            for(String hashedImage : hashedImages){
                if(hashedImage.isEmpty()){
                    continue;
                }


                String imageUrl;
                if(IMAGE_MAP.containsKey(hashedImage)){
                    imageUrl = IMAGE_MAP.get(hashedImage);
                } else{
                    byte[] b = KVS_CLIENT.get(IMAGE_MAPPING_TABLE, hashedImage, PropertyLoader.getProperty("table.default.value"));
                    if(b != null){
                        imageUrl = new String(b);
                        IMAGE_MAP.put(hashedImage, imageUrl);
                    } else{
                        continue;
                    }
                }

                if(result.containsKey(fromUrlId)){
                    result.get(fromUrlId).add(imageUrl);
                } else{
                    Set<String> urls = new HashSet<>();
                    urls.add(imageUrl);
                    result.put(fromUrlId, urls);
                }
            }
        }
        return result;
    }




}