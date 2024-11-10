package cis5550.webserver.pool;

import cis5550.webserver.Server;
import cis5550.tools.Logger;

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
        Thread[] pool = new Thread[cis5550.webserver.Server.NUM_WORKERS];

        log.info("Creating thread pool with " + cis5550.webserver.Server.NUM_WORKERS + " threads");

        for(int i = 0; i < Server.NUM_WORKERS; i++) {
            pool[i] = new Thread(new Worker(taskQueue));
            pool[i].start();
        }
    }

    public void execute(Runnable task) throws InterruptedException {
        taskQueue.put(task);
    }


}
