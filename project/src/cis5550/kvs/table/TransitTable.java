package cis5550.kvs.table;

import cis5550.kvs.Row;
import cis5550.kvs.version.Version;
import cis5550.kvs.version.VersionManager;
import cis5550.tools.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Xuanhe Zhang
 *
 * This class is a table that stores rows in a map in memory.
 * @syncrhonized is used to ensure that only one thread can access the table at a time.
 */
public class TransitTable implements Table{

    private static final Logger log = Logger.getLogger(TransitTable.class);

    private String key;
    private final Map<String, VersionManager<Row>> MANAGER_MAP = new ConcurrentHashMap<>();

    private final TreeSet<String> SORTED_KEYS = new TreeSet<>();
    public TransitTable(String tableName){
        this.key = tableName;
    }

    @Override
    public synchronized String key(){
        return key;
    }


    // get the last version of the row
    // return null if the row does not exist

    @Override
    public Version<Row> getRow(String rowName){
        VersionManager<Row> manager =  MANAGER_MAP.getOrDefault(rowName , null);
        // if the row does not exist, return null directly since we do not need to create a new row
        if(manager == null){
            log.info("row not found in table");
            return null;
        }
        // otherwise, return the last version of the row
        Version<Row> lastVersion = manager.getLastVersion();
        if(lastVersion == null){
            log.info("row is null in table");
            return null;
        }
        log.info("getting data in table: " + key + " row: " + rowName);
        return lastVersion;
    }

    // since this method supports versioning, it is important to create a new version of the row first.
    // since there is a versioning, this method may not properly describe the behavior of the method
    @Deprecated
    public synchronized Version<Row> addRow(String rowName){
        // create a new row if the row does not exist
        if(MANAGER_MAP.containsKey(rowName)){
            return MANAGER_MAP.get(rowName).getNewVersion();
        }
        // create a new version manager for the row
        VersionManager<Row> manager = new VersionManager<>(new Row(rowName));
        MANAGER_MAP.put(rowName, manager);


        return manager.getLastVersion();
    }

    @Override
    public synchronized void putRow(Row row){
        // put the row in the table
        if(MANAGER_MAP.containsKey(row.key())){
            MANAGER_MAP.get(row.key()).putNewVersion(row);
            return;
        }
        // create a new version manager for the row
        VersionManager<Row> manager = new VersionManager<>(row);
        log.info("putting data in table: " + key + " row: " + row.key());
        MANAGER_MAP.put(row.key(), manager);
    }

    // since this method supports versioning, put row will create a new version of the row
    @Override
    public synchronized void putRow(String rowName, Row row) {
        if(MANAGER_MAP.containsKey(rowName)){
            MANAGER_MAP.get(rowName).putNewVersion(row);
            return;
        }
        // create a new version manager for the row
        VersionManager<Row> manager = new VersionManager<>(row);
        log.info("putting data in table: " + key + " row: " + rowName);
        MANAGER_MAP.put(rowName, manager);
    }

    @Override
    public synchronized Version<Row> putRow(String rowName) {
        if(MANAGER_MAP.containsKey(rowName)){
            return MANAGER_MAP.get(rowName).getNewVersion();
        }
        // create a new version manager for the row
        VersionManager<Row> manager = new VersionManager<>(new Row(rowName));
        log.info("putting data in table: " + key + " row: " + rowName);
        MANAGER_MAP.put(rowName, manager);
        return manager.getLastVersion();
    }

    @Override
    public Version<Row> getRow(String rowName, String versionKey){

        // get the version manager of the row
        VersionManager<Row> manager = MANAGER_MAP.getOrDefault(rowName, null);
        // if the row does not exist, return null
        if(manager == null){
            return null;
        }

        if(versionKey == null){
            log.info("specific version not found, return the last version");
            return manager.getLastVersion();
        }
        // get the version of the row by version number
        return manager.getVersion(versionKey);
    }

    @Override
    public List<Row> getRows(String startKey, String endKeyExclusive) {
        if(startKey == null){
            log.info("start key is null, set to empty string");
            startKey = "";
        }

        // create a list to store the rows
        List<Row> rows = new Vector<>();
        // iterate through the version managers to get the rows
        for (Map.Entry<String, VersionManager<Row>> entry : MANAGER_MAP.entrySet()) {
            log.info("[get rows] getting data in table: " + key + " row: " + entry.getKey());
            // get the last version of the row
            VersionManager<Row> manager = entry.getValue();
            Row row = manager.getLastVersion().getValue();
            // if the row is in the range, add the row to the list
            if(row.key().compareTo(startKey) >= 0
                    && (endKeyExclusive == null || row.key().compareTo(endKeyExclusive) < 0)) {
                log.info("[scan] | " + key  + " | " + manager.getLastVersion().getValue().key() + " | " + startKey + " | " + endKeyExclusive);
                rows.add(manager.getLastVersion().getValue());
            }
        }

        log.info("[scan] | " + rows.size() + " rows");
        return rows;
    }


    @Override
    public int count(){
        // return the number of rows in the table
        return MANAGER_MAP.size();
    }

    @Override
    public synchronized Vector<Row> getSortedRows(String startKey, int limit){

        SORTED_KEYS.addAll(MANAGER_MAP.keySet());
        log.info("sorted keys updated");

        Vector<Row> rows = new Vector<>();

        Iterator<String> iterator = SORTED_KEYS.tailSet(startKey).iterator();

        for(int i = 0; i < limit && iterator.hasNext(); i++){
            String key = iterator.next();
            VersionManager<Row> manager = MANAGER_MAP.get(key);
            rows.add(manager.getLastVersion().getValue());
        }

        return rows;
    }


    @Override
    public void clear(){
        // clear the table
        MANAGER_MAP.clear();
    }

    @Override
    public synchronized boolean rename(String key){
        // rename the table
        this.key = key;
        return true;
    }
}
