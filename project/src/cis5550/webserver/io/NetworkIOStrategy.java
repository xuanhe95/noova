package cis5550.webserver.io;

import java.io.IOException;

public abstract class NetworkIOStrategy implements IOStrategy, NetworkConnection {
    abstract public void closeAll() throws IOException;

    abstract public boolean isSecure();
}
