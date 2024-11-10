package cis5550.webserver.io;

import java.io.*;

/**
 * @author Xuanhe Zhang
 */
public class ByteArrayIOStrategy implements IOStrategy{
    private final ByteArrayInputStream inputStream;
    private final ByteArrayOutputStream outputStream;
    public ByteArrayIOStrategy(String data) {
        this.outputStream = new ByteArrayOutputStream();
        this.inputStream = new ByteArrayInputStream(data.getBytes());
    }
    public ByteArrayIOStrategy(byte[] data) {
        this.outputStream = new ByteArrayOutputStream();
        this.inputStream = new ByteArrayInputStream(data);
    }

    @Override
    public InputStream in() throws IOException {
        return inputStream;
    }

    @Override
    public OutputStream out() throws IOException{
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
        outputStream.write(data);
    }

    @Override
    public int read(byte[] data) throws IOException {
        return inputStream.read(data);
    }

    @Override
    public int read() throws IOException {
        return inputStream.read();
    }

    @Override
    public int read(byte[] data, int offset, int length) throws IOException {
        return inputStream.read(data, offset, length);
    }

    @Override
    public void write(byte[] data, int offset, int length) throws IOException {
        outputStream.write(data, offset, length);
    }
}
