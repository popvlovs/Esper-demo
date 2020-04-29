package espercep.demo.cases.ipc.chronicle;

import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/4/8
 */
public class BackpressureHandler {
    private static final Logger logger = LoggerFactory.getLogger(BackpressureHandler.class);

    private CoordinatorStore coordinatorStore;
    private SingleChronicleQueue ipc;
    private ExcerptAppender appender;

    private AtomicBoolean hasBackPressure = new AtomicBoolean(false);
    private static final long MAX_LAG = 1_000_000L;

    private AtomicBoolean isRunning = new AtomicBoolean(false);

    public BackpressureHandler(CoordinatorStore coordinatorStore, SingleChronicleQueue ipc, ExcerptAppender appender) {
        this.coordinatorStore = coordinatorStore;
        this.ipc = ipc;
        this.appender = appender;
    }

    public void checkBackPressure() {
        Collection<ConsumerMetaInfo> consumers = coordinatorStore.availableConsumers();
        long producerIndex = appender.lastIndexAppended();
        boolean isAnyLagExceeded = false;
        for (ConsumerMetaInfo consumer: consumers) {
            try {
                long consumerIndex = consumer.getIndex();
                long gap = ipc.countExcerpts(producerIndex, consumerIndex);
                if (gap > MAX_LAG) {
                    isAnyLagExceeded = true;
                }
                logger.info("Lag of {}: {}", consumer, gap);
            } catch (Exception e) {
                logger.warn("Lag of {}: {}", consumer, e.getMessage());
            }
        }
        hasBackPressure.set(isAnyLagExceeded);
    }

    public void startPeriodicCheck() {
        if (isRunning.compareAndSet(false, true)) {
            logger.info("Start to scheduled check back-pressure");
            Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(this::checkBackPressure, 0L, 1, TimeUnit.SECONDS);
        }
    }

    public boolean shouldWaitForBackPressure() {
        return hasBackPressure.get();
    }
}
