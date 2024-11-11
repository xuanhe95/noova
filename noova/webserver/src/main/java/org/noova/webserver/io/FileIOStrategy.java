package org.noova.webserver.io;

import java.io.*;

/**
 * @author Xuanhe Zhang
 */
public class FileIOStrategy implements IOStrategy{
    private final File file;
    private FileOutputStream outputStream;
    private FileInputStream inputStream;
    public FileIOStrategy(File file) throws FileNotFoundException {
        this.file = file;
    }

    @Override
    public InputStream in() throws IOException {
        if(inputStream == null){
            inputStream = new FileInputStream(file);
        }
        return inputStream;
    }

    @Override
    public OutputStream out() throws IOException {
        if(outputStream == null){
            outputStream = new FileOutputStream(file);
        }
        return outputStream;
    }

    @Override
    public void close() throws IOException {
        if(outputStream != null){
            outputStream.close();
        }
        if(inputStream != null){
            inputStream.close();
        }
    }

    @Override
    public void write(byte[] data) throws IOException {
        if(outputStream == null){
            outputStream = new FileOutputStream(file);
        }
        outputStream.write(data);
    }

    @Override
    public int read(byte[] data) throws IOException {
        if(inputStream == null){
            inputStream = new FileInputStream(file);
        }
        return inputStream.read(data);
    }

    @Override
    public int read() throws IOException {
        if(inputStream == null){
            inputStream = new FileInputStream(file);
        }
        return inputStream.read();
    }

    @Override
    public int read(byte[] data, int offset, int length) throws IOException {
        if(inputStream == null){
            inputStream = new FileInputStream(file);
        }
        return inputStream.read(data, offset, length);
    }

    @Override
    public void write(byte[] data, int offset, int length) throws IOException {
        if(outputStream == null){
            outputStream = new FileOutputStream(file);
        }
        outputStream.write(data, offset, length);
    }



}
