package espercep.demo.util;

import net.openhft.affinity.AffinityLock;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/3/27
 */
public class AffinityThreadFactory implements ThreadFactory {
    private static final AtomicInteger poolNumber = new AtomicInteger(1);
    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(0);
    private final String namePrefix;
    private int[] availableCores = IntStream.range(8, 24).toArray();

    public AffinityThreadFactory(int[] availableCores) {
        this.availableCores = availableCores;
        SecurityManager s = System.getSecurityManager();
        group = (s != null) ? s.getThreadGroup() :
                Thread.currentThread().getThreadGroup();
        namePrefix = "esper-engine-pool-" + poolNumber.getAndIncrement() + "-thread-";
    }

    public AffinityThreadFactory() {
        SecurityManager s = System.getSecurityManager();
        group = (s != null) ? s.getThreadGroup() :
                Thread.currentThread().getThreadGroup();
        namePrefix = "esper-engine-pool-" + poolNumber.getAndIncrement() + "-thread-";
    }

    public Thread newThread(Runnable r) {
        int threadIdx = threadNumber.getAndIncrement();
        Runnable threadAffinityRunnable = new Runnable() {
            @Override
            public void run() {
                if (availableCores.length == 0) {
                    try (AffinityLock affinityLock = AffinityLock.acquireLock()) {
                        r.run();
                    }
                } else {
                    try (AffinityLock affinityLock = AffinityLock.acquireLock(availableCores[threadIdx % availableCores.length])) {
                        r.run();
                    }
                }
            }
        };

        Thread t = new Thread(group, threadAffinityRunnable,
                namePrefix + threadIdx,
                0);
        if (t.isDaemon())
            t.setDaemon(true);
        if (t.getPriority() != Thread.NORM_PRIORITY)
            t.setPriority(Thread.NORM_PRIORITY);
        return t;
    }

}
