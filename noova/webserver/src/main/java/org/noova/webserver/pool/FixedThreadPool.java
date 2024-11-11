package org.noova.webserver.pool;

import org.noova.tools.Logger;
import org.noova.webserver.Server;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Xuanhe Zhang
 */
public class FixedThreadPool implements ThreadPool {
    private static final Logger log = Logger.getLogger(FixedThreadPool.class);
    private final BlockingQueue<Runnable> taskQueue;

    private final AtomicInteger count = new AtomicInteger(0);

    public FixedThreadPool(int maxTasks) {
        taskQueue = new ArrayBlockingQueue<>(maxTasks);
        Thread[] pool = new Thread[org.noova.webserver.Server.NUM_WORKERS];

        log.info("Creating thread pool with " + org.noova.webserver.Server.NUM_WORKERS + " threads");

        for(int i = 0; i < Server.NUM_WORKERS; i++) {
            pool[i] = new Thread(new Worker(taskQueue));
            pool[i].start();
        }
    }

    public void execute(Runnable task) throws InterruptedException {
        taskQueue.put(task);
    }


}
