package org.noova.webserver.io;

import java.net.InetSocketAddress;

public interface NetworkConnection {
    InetSocketAddress getRemoteAddress();
}
