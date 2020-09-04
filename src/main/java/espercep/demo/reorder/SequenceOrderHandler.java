package espercep.demo.reorder;

import espercep.demo.util.MetricUtil;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 * <p>
 * 引入类似flink的watermark机制，在日志进入esper engine前做前置正序
 *
 * @author yitian_song 2019/12/31
 */

public class SequenceOrderHandler {
    private static final Logger logger = LoggerFactory.getLogger(SequenceOrderHandler.class);
    /**
     * 最大保持时间，即最大能够处理的乱序范围
     */
    private long maxRetainedMillis;

    /**
     * 最大队列长度，超出后自动dequeue，避免OOM
     * 默认情况下是保证60w eps * maxRetainedMillis的长度
     */
    private long maxQueueSize;

    /**
     * 本地内存缓存队列，保存事件，最大长度通过maxQueueSize进行控制，超出长度后dequeue直到80%
     */
    private final Map<Long, List<Map<String, Object>>> elementQueue = new ConcurrentHashMap<>();

    /**
     * 正序后事件出列action，在这里基本就是进Esper engine
     */
    private Consumer<Map<String, Object>> onDeque;

    /**
     * 用于记录elementQueue的长度，因为是Map + List的形式，实时统计不太方便
     */
    private AtomicLong queueSizeCnt = new AtomicLong(0L);

    /**
     * 异步记录系统时间（低精度），精度取决于SchedulerService的period，默认是200ms
     * 主要是为了防止过于频繁调用系统时间接口，导致不必要的性能开销
     */
    private AtomicLong currentTimeMillis = new AtomicLong(0L);

    /**
     * 记录当前时刻为止，eventTime的最大值，然后在定时器中作为watermark触发事件dequeue
     */
    private volatile long maxTimestamp = 0L;

    /**
     * 最大空闲间隔，避免长时间没有watermark生成时，事件被无限pending
     */
    private long maxFlushAllIntervalMillis;

    /**
     * 记录最后一次更新watermark的时间，长时间未更新watermark的话，触发一次flush all
     */
    private volatile long updateWatermarkTimestamp = 0L;

    /**
     * 用于统计输出时间下降沿的个数，表征未被正确处理的乱序数
     */
    private volatile long lastOutputTimestamp = 0L;

    /**
     * 表征事件时间的属性字段，默认基于occur_time字段进行正序，
     * 要求输入的event类型中必须有该字段，如果没有则直接丢弃
     */
    private String orderByField;

    private AtomicBoolean isTimerUpdated = new AtomicBoolean(false);

    public SequenceOrderHandler(long maxRetainedMillis,
                                long maxQueueSize,
                                long maxFlushAllIntervalMillis,
                                String orderByField) {
        this.maxRetainedMillis = maxRetainedMillis;
        this.maxQueueSize = maxQueueSize;
        this.maxFlushAllIntervalMillis = maxFlushAllIntervalMillis;
        this.orderByField = orderByField;
    }

    public long getQueueSize() {
        return queueSizeCnt.get();
    }

    /**
     * Can only execute once
     */
    public void start(Consumer<Map<String, Object>> onDeque) {
        if (this.onDeque == null) {
            this.onDeque = onDeque;
            initScheduledThread();
        }
    }

    /**
     * Can only execute once
     */
    private void initScheduledThread() {
        // Init timer
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor(new BasicThreadFactory.Builder()
                .namingPattern("sequence-order-handler-timer")
                .daemon(true)
                .build());
        service.scheduleAtFixedRate(() -> {
            isTimerUpdated.set(true);
            currentTimeMillis.set(System.currentTimeMillis());
        }, 1000, 100, TimeUnit.MILLISECONDS);
        Runtime.getRuntime().addShutdownHook(new Thread(service::shutdown));

        // Init out-of-order handler
        ExecutorService outputService = Executors.newSingleThreadExecutor(new BasicThreadFactory.Builder()
                .daemon(true)
                .namingPattern("sequence-order-handler-output")
                .build());
        outputService.submit(() -> {
            while (!Thread.interrupted()) {
                if (isTimerUpdated.compareAndSet(true, false)) {
                    try {
                        if ((updateWatermarkTimestamp > 0L) && (currentTimeMillis.get() > updateWatermarkTimestamp + maxFlushAllIntervalMillis)) {
                            // flush all
                            onEventTime(maxTimestamp + maxRetainedMillis);
                        } else {
                            onEventTime(maxTimestamp);
                        }
                    } catch (Exception e) {
                        logger.error("Error on handle watermark: ", e);
                    }
                }
            }
        });
        Runtime.getRuntime().addShutdownHook(new Thread(outputService::shutdown));
    }

    /**
     * Buffer event that contains `occur_time`, otherwise drop it
     */
    public void bufferEvent(Map<String, Object> element) {
        if (element.containsKey(orderByField)) {
            Object value = element.get(orderByField);
            if (value != null) {
                if (value instanceof String) {
                    bufferEvent(element, Long.parseLong((String) value));
                } else {
                    bufferEvent(element, (Long) value);
                }
            }
        }
    }


    /**
     * Buffer event with timestamp
     */
    private volatile long prevTimestamp = 0L;

    private void bufferEvent(Map<String, Object> element, long timestamp) {
        if (element == null) {
            return;
        }
        while (queueSizeCnt.get() > maxQueueSize) {
            // spin lock and waiting...
        }
        MetricUtil.getCounter("out-of-order-in").inc();
        // 1. queue event
        List<Map<String, Object>> elements = elementQueue.get(timestamp);
        if (elements == null) {
            synchronized (elementQueue) {
                if ((elements = elementQueue.get(timestamp)) == null) {
                    elements = Collections.synchronizedList(new ArrayList<>());
                    elementQueue.put(timestamp, elements);
                }
            }
        }
        elements.add(element);
        queueSizeCnt.incrementAndGet();

        // 2. set current watermark
        if (timestamp >= maxTimestamp) {
            maxTimestamp = timestamp;
            updateWatermarkTimestamp = currentTimeMillis.get();
        }

        // 3. count out-of-range events
        if (prevTimestamp > 0L && prevTimestamp > (timestamp + maxRetainedMillis)) {
            MetricUtil.getCounter("out-of-order-exceed-limit").inc();
        }
        prevTimestamp = timestamp;
    }

    private synchronized void onEventTime(long timestamp) {
        // 1. handle low watermark
        advanceTime(timestamp - maxRetainedMillis);

        // 2. handle queue size limit exceeded
        if (queueSizeCnt.get() >= maxQueueSize) {
            PriorityQueue<Long> orderedTimestamp = getOrderedTimestamp();
            while (!orderedTimestamp.isEmpty() && (queueSizeCnt.get() >= maxQueueSize * 0.8)) {
                advanceTime(orderedTimestamp.poll());
            }
        }
    }

    private void advanceTime(long watermark) {
        PriorityQueue<Long> orderedTimestamp = getOrderedTimestamp();
        while (!orderedTimestamp.isEmpty() && orderedTimestamp.peek() <= watermark) {
            long timestamp = orderedTimestamp.poll();

            List<Map<String, Object>> queue = elementQueue.remove(timestamp);

            // Log how many out-of-order data after handler
            if (timestamp < lastOutputTimestamp) {
                MetricUtil.getCounter("out-of-order-after-queue").inc(queue.size());
            }
            lastOutputTimestamp = timestamp;
            MetricUtil.getCounter("out-of-order-out").inc(queue.size());
            try (Stream<Map<String, Object>> elements = queue.stream()) {
                elements.forEach(
                        event -> {
                            try {
                                onDeque.accept(event);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                );
            }
            queueSizeCnt.addAndGet(-queue.size());
        }
    }

    private PriorityQueue<Long> getOrderedTimestamp() {
        return new PriorityQueue<>(elementQueue.keySet());
    }
}
