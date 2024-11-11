package org.noova.webserver.pool;

public interface ThreadPool {
    void execute(Runnable task) throws InterruptedException;
}
