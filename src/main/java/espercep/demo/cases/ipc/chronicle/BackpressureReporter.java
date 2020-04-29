package espercep.demo.cases.ipc.chronicle;

import net.openhft.chronicle.queue.ExcerptTailer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/4/8
 */
public class BackpressureReporter {
    private static final Logger logger = LoggerFactory.getLogger(BackpressureReporter.class);

    private CoordinatorStore coordinatorStore;
    private ExcerptTailer tailer;
    private String consumerId;

    public BackpressureReporter(CoordinatorStore coordinatorStore, ExcerptTailer tailer, String consumerId) {
        this.coordinatorStore = coordinatorStore;
        this.tailer = tailer;
        this.consumerId = consumerId;
    }

    private AtomicBoolean isRunning = new AtomicBoolean(false);

    public void reportTailerIndex() {
        ConsumerMetaInfo consumer = coordinatorStore.getOrElse(consumerId, ConsumerMetaInfo::new);
        consumer.setId(consumerId);
        consumer.setHeartbeat(System.currentTimeMillis());
        consumer.setIndex(tailer.index());
        coordinatorStore.register(consumer);
    }

    public void startPeriodicReport() {
        if (isRunning.compareAndSet(false, true)) {
            logger.info("Start to scheduled check back-pressure");
            Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(this::reportTailerIndex, 0L, 1, TimeUnit.SECONDS);
        }
    }
}
