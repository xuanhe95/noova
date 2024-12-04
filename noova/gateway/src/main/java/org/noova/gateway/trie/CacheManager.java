package org.noova.gateway.trie;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.noova.gateway.storage.StorageStrategy;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class CacheManager {

    private static final String CACHE_DIRECTORY = "__cache/";

    private static final StorageStrategy STORAGE_STRATEGY = StorageStrategy.getInstance();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static CacheManager instance;

    private CacheManager() {
    }

    public static CacheManager getInstance() {
        if (instance == null) {
            instance = new CacheManager();
        }
        return instance;
    }

    public void saveCache(String key, Map cache) throws IOException {
        // Save cache
        byte[] json = OBJECT_MAPPER.writeValueAsBytes(cache);
        // 确保目录存在
        Path directory = Paths.get(CACHE_DIRECTORY);
        if (!Files.exists(directory)) {
            Files.createDirectories(directory);
        }

        // 构建文件路径
        Path filePath = Paths.get(CACHE_DIRECTORY, key + ".cache");

        // 保存到文件
        try (FileOutputStream fos = new FileOutputStream(filePath.toFile())) {
            fos.write(json);
        }

        System.out.println("Cache saved to: " + filePath);

    }

    public Map getCache(String key) throws IOException {
        // 构建文件路径
        Path filePath = Paths.get(CACHE_DIRECTORY, key + ".cache");

        // 检查文件是否存在
        if (!Files.exists(filePath)) {
            System.out.println("Cache file not found for key: " + key);
            return null;
        }

        // 读取文件内容
        byte[] jsonBytes = Files.readAllBytes(filePath);

        // 将 JSON 字节数组解析为 Map
        return OBJECT_MAPPER.readValue(jsonBytes, Map.class);
    }


}
