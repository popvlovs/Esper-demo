package espercep.demo.cases.ipc.chronicle;

import com.alibaba.fastjson.JSONObject;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/4/8
 */
public class ChronicleMessageChannel<T> {
    private static final Logger logger = LoggerFactory.getLogger(ChronicleMessageChannel.class);
    private static final String STORE_PATH_FORMAT = "/dev/shm/ipc-topic/{0}";

    private String id;
    private String topic;
    private ChronicleMessageRole role;
    private String storeFilePath;
    private CoordinatorStore coordinatorStore;
    private Class<T> clazz;

    public ChronicleMessageChannel(String id, String topic, ChronicleMessageRole role, Class<T> clazz) {
        this.id = id;
        this.topic = topic;
        this.role = role;
        this.clazz = clazz;
        this.storeFilePath = MessageFormat.format(STORE_PATH_FORMAT, topic);
        this.coordinatorStore = CoordinatorStore.instance(topic);
        this.coordinatorStore.registerOnStartup(id);
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    }

    private void close() {
        logger.info("Close chronicle message channel: {}", this);
        coordinatorStore.delete(id);
    }

    public Collection<ConsumerMetaInfo> availableConsumers() {
        return coordinatorStore.availableConsumers();
    }

    public void consume(Consumer<T> action) {
        if (role != ChronicleMessageRole.CONSUMER) {
            throw new RuntimeException("Unsupported consume operation");
        }

        ConsumerStoreFileListener storeFileListener = new ConsumerStoreFileListener(id, coordinatorStore);
        try (ChronicleQueue ipc = SingleChronicleQueueBuilder.single(storeFilePath)
                .rollCycle(RollCycles.SECONDLY_5)
                .storeFileListener(storeFileListener)
                .build()) {

            logger.info("Start consumer: {}", this);
            ExcerptTailer tailer = ipc.createTailer(id);
            storeFileListener.setTailer(tailer);

            BackpressureReporter backpressureReporter = new BackpressureReporter(coordinatorStore, tailer, id);
            backpressureReporter.startPeriodicReport();

            // Consumer loop
            while (!Thread.currentThread().isInterrupted()) {
                String text;
                if ((text = tailer.readText()) != null) {
                    T data = JSONObject.parseObject(text, clazz);
                    action.accept(data);
                } else {
                    // TODO: apply waiting strategy?
                }
            }
        }
    }

    public void produce(Supplier<T> supplier) {
        if (role != ChronicleMessageRole.PRODUCER) {
            throw new RuntimeException("Unsupported producer operation");
        }

        // Create coordinator store for back-pressure between producer/consumer(s)
        CoordinatorStore coordinatorStore = CoordinatorStore.instance(topic);
        ProducerStoreFileListener storeFileListener = new ProducerStoreFileListener(coordinatorStore);
        try (SingleChronicleQueue ipc = SingleChronicleQueueBuilder.single(storeFilePath)
                .rollCycle(RollCycles.SECONDLY_5)
                .storeFileListener(storeFileListener)
                .build()) {
            logger.info("Start producer: {}", this);
            ExcerptAppender appender = ipc.acquireAppender();

            // Scheduled check back-pressure
            BackpressureHandler backpressureHandler = new BackpressureHandler(coordinatorStore, ipc, appender);
            backpressureHandler.startPeriodicCheck();

            while (!Thread.currentThread().isInterrupted()) {
                if (backpressureHandler.shouldWaitForBackPressure()) {
                    // TODO: apply waiting strategy
                    Thread.yield();
                } else {
                    T data = supplier.get();
                    if (data == null) {
                        return;
                    }
                    String text = JSONObject.toJSONString(data);
                    appender.writeText(text);
                }
            }
        }
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public String toString() {
        return "ChronicleMessageChannel{" +
                "id='" + id + '\'' +
                ", topic='" + topic + '\'' +
                ", role=" + role +
                ", storeFilePath='" + storeFilePath + '\'' +
                '}';
    }
}
