package org.noova.webserver.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author Xuanhe Zhang
 */
public interface IOStrategy extends AutoCloseable {

    InputStream in() throws IOException;
    OutputStream out() throws IOException;

    void write(byte[] data) throws IOException;

    int read(byte[] data) throws IOException;

    int read() throws IOException;

    int read(byte[] data, int offset, int length) throws IOException;

    void write(byte[] data, int offset, int length) throws IOException;

    public void close() throws IOException;


}
