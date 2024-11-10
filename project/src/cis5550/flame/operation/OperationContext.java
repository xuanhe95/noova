package cis5550.flame.operation;

import cis5550.kvs.KVS;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.KeyEncoder;
import cis5550.webserver.Request;
import cis5550.tools.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class OperationContext {
    private static final Logger log = Logger.getLogger(OperationContext.class);
    KVSClient kvs;

    File JAR;

    List<String> inputs = new ArrayList<>();
    String output;

    boolean pair;
    byte[] lambda;
    Iterator<Row> it;

    String startFrom;

    String endExclusive;


    Request req;

    public OperationContext(Request req) {
        log.warn("[context] kvs = " + req.queryParams("kvs"));
        this.kvs = new KVSClient(req.queryParams("kvs"));
    }


    public OperationContext(KVSClient kvs) {
        this.kvs = kvs;
    }

    public KVSClient getKVS() {
        return kvs;
    }

    public void from(String from){
        this.startFrom = from == null ? null : KeyEncoder.decode(from);
    }

    public void to(String to) {
        this.endExclusive = to == null ? null : KeyEncoder.decode(to);
    }

    public String from(){
        return startFrom;
    }

    public String to(){
        return endExclusive;
    }

    public void setJAR(File jar) {
        this.JAR = jar;
    }

    public File getJAR() {
        return JAR;
    }

    public void jar(File jar) {
        this.JAR = jar;
    }

    public void input(String input) {
        this.inputs.add(input);
    }

    public String input(int index) {
        return inputs.get(index);
    }

    public String input(){
        return inputs.get(0);
    }

    public void inputs(List<String> inputs) {
        this.inputs = inputs;
    }

    public List<String> inputs(){
        return inputs;
    }

    public void output(String output) {
        this.output = output;
    }

    public String output() {
        log.info("[context] output = " + output);
        return output;
    }

    public void pair(boolean pair) {
        this.pair = pair;
    }

    public boolean pair() {
        return pair;
    }

    public void lambda(byte[] lambda) {
        this.lambda = lambda;
    }

    public byte[] lambda() {
        return lambda;
    }

    public Iterator<Row> rows() {
        log.info("[context] get rows from = " + startFrom);
        log.info("[context] get rows to = " + endExclusive);
        try {
            return kvs.scan(input(), startFrom, endExclusive);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Iterator<Row> rows(int index) {
        log.info("[context] get rows from = " + startFrom);
        log.info("[context] get rows to = " + endExclusive);
        try {
            return kvs.scan(inputs.get(index), startFrom, endExclusive);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
