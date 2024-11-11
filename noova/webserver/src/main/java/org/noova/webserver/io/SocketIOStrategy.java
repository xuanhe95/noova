package org.noova.webserver.io;

import org.noova.tools.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * @author Xuanhe Zhang
 */
public class SocketIOStrategy extends NetworkIOStrategy {

    private static final Logger log = Logger.getLogger(SocketIOStrategy.class);
    private final Socket socket;
    private final InputStream inputStream;
    private final OutputStream outputStream;

    private boolean secure = false;


    public  SocketIOStrategy(Socket socket, boolean secure) throws IOException {
        this.socket = socket;
        this.inputStream = socket.getInputStream();
        this.outputStream = socket.getOutputStream();
        this.secure = secure;
    }

    @Override
    public boolean isSecure() {
        return secure;
    }

    @Override
    public InputStream in() throws IOException {
        return inputStream;
    }

    @Override
    public OutputStream out() throws IOException {
        return outputStream;
    }

    @Override
    public void write(byte[] data)  {
        try{
            outputStream.write(data);
            outputStream.flush();
        } catch (Exception e){
            log.error("Error writing to socket");
        }
    }


    @Override
    public int read(byte[] data) throws IOException {
        return inputStream.read(data);
    }

    @Override
    public void close() throws IOException {
        log.info("Closing socket");
        if(!socket.isClosed()){
            socket.shutdownOutput();
        }
    }

    @Override
    public void closeAll() throws IOException {
        log.info("Closing socket");
        socket.close();
    }


    @Override
    public InetSocketAddress getRemoteAddress() {
        return (InetSocketAddress) socket.getRemoteSocketAddress();
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

    public Socket getSocket() {
        return socket;
    }

}
