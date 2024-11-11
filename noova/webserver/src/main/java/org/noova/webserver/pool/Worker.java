package org.noova.webserver.pool;

import org.noova.tools.Logger;

import java.util.concurrent.BlockingQueue;

/**
 * @author Xuanhe Zhang
 */
public class Worker implements Runnable {
    private static final Logger log = Logger.getLogger(Worker.class);
    private final BlockingQueue<Runnable> taskQueue;
    public Worker(BlockingQueue<Runnable> taskQueue) {
        this.taskQueue= taskQueue;
    }

    /*
    * The worker will keep taking tasks from the queue and run them
     */
    public void run() {
        while (true) {
            try {
                Runnable task = taskQueue.take();
                task.run();
            } catch (InterruptedException e) {
                log.error("Interrupted");
            }
        }
    }
}
