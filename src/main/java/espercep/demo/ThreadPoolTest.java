package espercep.demo;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.*;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/7/15
 */
public class ThreadPoolTest {
    public static void main(String[] args) {
        ArrayBlockingQueue<Runnable> queue;
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("search-task-thread-prior-%d").build();
        ExecutorService executor = new ThreadPoolExecutor(10, 10,
                0, TimeUnit.NANOSECONDS,
                (queue = new ArrayBlockingQueue<>(100)),
                threadFactory,
                new ThreadPoolExecutor.AbortPolicy());
        Runtime.getRuntime().addShutdownHook(new Thread(executor::shutdown));
        Task task = new Task("task-1", queue);
        int i = 0;
        while (i++ < 1000) {
            executor.submit(task);
        }
    }

    private static class Task implements Runnable {
        private String name;
        private ArrayBlockingQueue queue;

        public Task(String name, ArrayBlockingQueue queue) {
            this.name = name;
            this.queue = queue;
        }

        @Override
        public void run() {
            System.out.println(this.name + " run");
            System.out.println("Queue size: " + queue.size());
            while (true);
        }
    }
}
