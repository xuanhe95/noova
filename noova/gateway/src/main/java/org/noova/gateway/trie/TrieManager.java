package org.noova.gateway.trie;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.noova.gateway.storage.StorageStrategy;
import org.noova.tools.Logger;
import org.noova.tools.PropertyLoader;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TrieManager {

    private static final StorageStrategy STORAGE_STRATEGY = StorageStrategy.getInstance();
    private static final Logger log = Logger.getLogger(TrieManager.class);
    private static TrieManager instance = null;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Map<String, Trie> TRIE_MAP = new HashMap<>();

    private TrieManager() {
    }

    public static TrieManager getInstance() {
        if (instance == null) {
            instance = new TrieManager();
        }
        return instance;
    }

    public Trie buildTrie(String originalTableName) throws IOException {
        log.info("[trie] Building trie: " + originalTableName);


        var it = STORAGE_STRATEGY.scan(originalTableName);
        if(it == null){
            log.error("[build trie] No data found");
        }
        log.info("[trie] Scanned table: " + originalTableName);

        Trie trie = new DistanceTrie();

        it.forEachRemaining(row -> {
            log.info("[trie] Inserting row: " + row.key());
            String value = row.key();
            String rawUrlsWithPositions = row.get(PropertyLoader.getProperty("table.index.acc"));
            trie.insert(value, rawUrlsWithPositions);
        });
        log.info("[trie] Trie built");

        return trie;
    }

    public void saveTrie(Trie trie, String rowName) throws IOException {
        log.info("[trie] Saving trie...");
        byte[] json;
        try {
            json = OBJECT_MAPPER.writeValueAsBytes(trie);
        } catch (Exception e) {
            log.error("[trie] Error converting trie to json");
            log.error(e.getMessage());
            return;
        }
        log.info("[trie] Json converted");

        STORAGE_STRATEGY.save(
                PropertyLoader.getProperty("table.trie"),
                rowName,
                PropertyLoader.getProperty("table.default.value"),
                json
        );
        System.out.println("Cache saved to table." );

        Path filePath = Paths.get(CACHE_DIRECTORY, "trieCache.cache");
        try (FileOutputStream fos = new FileOutputStream(filePath.toFile())) {
            fos.write(json);
        }

        System.out.println("Cache saved to: " + filePath);

    }

    private static final String CACHE_DIRECTORY = "__cache/";

    public Trie loadTrie(String rowName) throws IOException {
        log.info("[trie] Loading trie");
        if(TRIE_MAP.containsKey(rowName)){
            log.info("[trie] Trie found in cache");
            return TRIE_MAP.get(rowName);
        }

        Path filePath = Paths.get(CACHE_DIRECTORY, "trieCache.cache");
        String json = null;
        if (!Files.exists(filePath)) {
            System.out.println("Trie Cache file not found");
            json = STORAGE_STRATEGY.get(
                    PropertyLoader.getProperty("table.trie"),
                    rowName,
                    PropertyLoader.getProperty("table.default.value")
            );
            if(json == null){
                return null;
            }
        }else{
            byte[] jsonBytes = Files.readAllBytes(filePath);
            json = new String(jsonBytes);
            log.info("[trie] Json loaded from Cache");
        }

        log.info("[trie] Json loaded");
        DistanceTrie trie = OBJECT_MAPPER.readValue(json, DistanceTrie.class);
        log.info("[trie] Trie loaded");
        TRIE_MAP.put(rowName, trie);

        return trie;
    }
}