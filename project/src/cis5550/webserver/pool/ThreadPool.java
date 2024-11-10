package cis5550.webserver.pool;

public interface ThreadPool {
    void execute(Runnable task) throws InterruptedException;
}
