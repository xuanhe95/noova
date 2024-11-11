package org.noova.kvs.table;

import org.noova.kvs.Row;
import org.noova.kvs.version.Version;
import org.noova.tools.KeyEncoder;
import org.noova.tools.Logger;
import org.noova.webserver.io.FileIOStrategy;
import org.noova.webserver.io.IOStrategy;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Vector;

/**
 * @author Xuanhe Zhang
 */
public class PersistTable implements Table {

    private static final Logger log = Logger.getLogger(PersistTable.class);

    private static final int SUBDIR_LENGTH = 20;

    private static final String SUBDIR_PREFIX = "__";

    private static final int MAX_DIR_LENGTH = 5;

    String key;

    File rootFile;

    String rootFilePath;

    String storageDir;

    public PersistTable(String tableName, String storageDir){

        this.key = tableName;
        this.storageDir = storageDir;
        this.rootFile = new File(getStorageDirName(tableName));
        this.rootFilePath = getStorageDirName(tableName);

        if(!rootFile.exists()){
            boolean ok = rootFile.mkdirs();
            if(!ok){
                throw new RuntimeException("create rootFile failed");
            }
        }
    }

    @Override
    public List<Row> getRows(String startKey, String endKeyExclusive) {
        if(startKey == null){
            log.info("[persist table] start key is null, get all rows until " + endKeyExclusive);
            startKey = "";
        }


        List<Row> rows = new Vector<>();
        List<File> files = getAllFiles();
        log.info("[persist table] all file size: " + files.size());
        for(File f : files){
            String filename = f.getName();
            log.info("[persist table] filename: " + filename);
            if(filename.compareTo(startKey) >= 0
                    && (endKeyExclusive == null || filename.compareTo(endKeyExclusive) < 0)
                    && !f.isHidden()){
                log.info("[persist table] read from rootFile: " + f.getAbsolutePath());
                try {
                    IOStrategy io = new FileIOStrategy(f);
                    Row row = Row.readFrom(io.in());
                    if(row == null){
                        log.error("[persist table] row is null, ignore");
                        continue;
                    }
                    log.info("[persist table] add row: " + row.key());
                    rows.add(row);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        return rows;
    }

    private void getAllFilesRecursively(List<File> list, File rootFile){
        File[] files = rootFile.listFiles();
        if(files == null) {
            return;
        }
        for(File f : files){
            if(f.isDirectory()){
                getAllFilesRecursively(list, f);
            } else {
                list.add(f);
            }
        }
    }
    //    @Override
//    public List<Row> getRows(){
//        log.info("get all rows");
//        return getRows("", null);
//    }

    private FileIOStrategy getEncodedFile(String key){

        String encodedKey = KeyEncoder.encode(key);
        File tableFile = new File(rootFilePath, encodedKey);

        // this if for EC, if the key is too long, we need to create a subdir
        if(encodedKey.length() > MAX_DIR_LENGTH){
            String subdir = SUBDIR_PREFIX + encodedKey.substring(0, SUBDIR_LENGTH);
            File subFile = new File(rootFilePath + "/" + subdir);
            if(!subFile.exists()){
                boolean ok = subFile.mkdirs();
                if(!ok){
                    throw new RuntimeException("create rootFile failed");
                }
            }
            tableFile = new File(subFile, encodedKey);
        }

        log.info("get rootFile: " + tableFile.getAbsolutePath());

        if(!tableFile.exists()){
            try{
                boolean ok = tableFile.createNewFile();
                if(!ok){
                    throw new RuntimeException("create rootFile failed");
                }
            } catch(IOException e){
                throw new RuntimeException(e);
            }
        }

        try {
            return new FileIOStrategy(tableFile);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public String key(){
        return key;
    }


    @Override
    public Version<Row> getRow(String key) {

        IOStrategy io = getEncodedFile(key);

        try{
            Row row = Row.readFrom(io.in());
            log.info("read from rootFile");
            if(row == null){
                return new Version<>("0", new Row(key));
            }
            return new Version<>("0", row);
        } catch(Exception e) {
            log.error("Error reading from rootFile");
            throw new RuntimeException(e);
        }

    }

    @Override
    public synchronized void putRow(Row row) {
        IOStrategy io = getEncodedFile(row.key());
        try {
            io.write(row.toByteArray());
            log.info("write to rootFile");
        } catch (IOException e) {
            log.error("Error writing to rootFile");
            throw new RuntimeException(e);
        }
    }


    @Override
    public synchronized void putRow(String rowName, Row row) {

        IOStrategy io = getEncodedFile(rowName);
        try {
            io.write(row.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized Version<Row> putRow(String key){

        IOStrategy io = getEncodedFile(key);
        try{
            Row row = Row.readFrom(io.in());
            if(row == null){
                row = new Row(key);
            }
            io.write(row.toByteArray());
            return new Version<Row>("0", row);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

//    public synchronized void deleteRow(String rowName) {
//        String filename = KeyEncoder.encode(rowName);
//        File file = new File(filename);
//        if(!file.exists()){
//            return;
//        }
//        if(!file.delete()){
//            throw new RuntimeException("delete rootFile failed");
//        }
//    }

    @Override
    public synchronized Version<Row> getRow(String key, String version){
        return getRow(key);
    }



    @Override
    public int count() {
        List<File> files = getAllFiles();
        files.removeIf(File::isHidden);
        return files.size();
    }

//    public List<String> getTableKeys() {
//        File[] files = rootFile.listFiles();
//
//        List<String> keys = new Vector<>();
//        if(files == null){
//            return keys;
//        }
//
//        for(File file : files){
//            keys.add(file.getName());
//        }
//        return keys;
//    }

    private List<File> getAllFiles(){
        List<File> files = new Vector<>();
        getAllFilesRecursively(files, this.rootFile);
        return files;
    }

    @Override
    public Vector<Row> getSortedRows(String startKey, int limit){
//        List<File> files = getAllFiles();
//        Vector<Row> rows = new Vector<>();
//        files.sort((a, b) -> a.getName().compareTo(b.getName()));
//
//        if(startKey == null){
//            log.info("[persist table] start key is null, set to empty string");
//            startKey = "";
//        }
//
//        log.info("[persist table] rows size: " + files.size());
//        log.info("[persist table] start key: " + startKey);
//
//        for(File f : files){
//            String filename = f.getName();
//            if(filename.compareTo(startKey) >= 0 && !f.isHidden()){
//                try {
//                    IOStrategy io = new FileIOStrategy(f);
//                    Row row = Row.readFrom(io.in());
//                    log.info("[persist table] read from rootFile: " + f.getAbsolutePath());
//                    rows.add(row);
//                    if(rows.size() >= limit){
//                        break;
//                    }
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
//            }
//        }
//

        List<Row> rows = getRows(startKey, null);
        log.info("[persist table] rows size: " + rows.size());
        rows.sort((a, b) -> a.key().compareTo(b.key()));
        Vector<Row> vector = new Vector<>();
        for(int i = 0; i < Math.min(rows.size(), limit); i++){
            vector.add(rows.get(i));
        }
        return vector;
    }

    @Override
    public void clear(){
        List<File> files = getAllFiles();
        for (File file : files) {
            boolean ok = file.delete();
            if(!ok){
                throw new RuntimeException("delete rootFile failed");
            }
        }
        boolean ok = rootFile.delete();
        if(!ok){
            throw new RuntimeException("delete rootFile failed");
        }
    }
    private boolean moveDirectory(File source, File target) {
        if (!target.exists()) {
            target.mkdirs();
        }

        for (File file : source.listFiles()) {
            File newTarget = new File(target, file.getName());
            try {
                if (file.isDirectory()) {
                    if (!moveDirectory(file, newTarget)) {
                        return false;
                    }
                } else {
                    Files.move(file.toPath(), newTarget.toPath(), StandardCopyOption.REPLACE_EXISTING);
                }
            } catch (IOException e) {
                log.error("Error moving file", e);
                return false;
            }
        }
        return true;
    }
    @Override
    public synchronized boolean rename(String key){
        this.key = key;
        File newFile = new File(getStorageDirName(key));
        this.rootFilePath = getStorageDirName(key);
        if (moveDirectory(rootFile, newFile)) {
            this.rootFile = newFile;
            return true;
        }
        return false;
    }

    private String getStorageDirName(String key){
        return storageDir + "/" + key;
    }

}
