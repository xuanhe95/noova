package cis5550.webserver.io;

import java.net.InetSocketAddress;

public interface NetworkConnection {
    InetSocketAddress getRemoteAddress();
}
