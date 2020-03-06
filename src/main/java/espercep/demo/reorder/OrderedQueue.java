package espercep.demo.reorder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2019/12/25
 * <p>
 * 对输入的时间序列数据进行缓存和正序
 */
public class OrderedQueue {

    /**
     * 最大保持时间，即最大能够处理的乱序范围
     */
    private long maxRetainedMillis = TimeUnit.SECONDS.toMillis(3);

    /**
     * 最大队列长度，超出后自动deque，避免OOM
     * 默认情况下是保证50w eps * maxRetainedMillis的长度
     */
    private long maxQueueSize = TimeUnit.MILLISECONDS.toSeconds(maxRetainedMillis) * 1_800_000L;

    /**
     * 最大空闲间隔，避免长时间没有watermark生成
     */
    private long maxFlushAllIntervalMillis = TimeUnit.SECONDS.toMillis(3);

    private Map<Long, List<Map<String, Object>>> elementQueue = new ConcurrentHashMap<>();
    private Consumer<Map<String, Object>> onDeque;

    private AtomicLong queueSizeCnt = new AtomicLong(0L);

    private volatile AtomicLong currentTimeMillis = new AtomicLong(0L);

    /**
     * 记录最新的eventTime，然后在定时器中触发watermark处理
     */
    private volatile long maxTimestamp = 0L;

    /**
     * 记录最后一次更新watermark的时间，长时间未更新watermark的话，触发一次flush all
     */
    private volatile long updateWatermarkTimestamp = 0L;

    private volatile long lastOutTimestamp = 0L;

    private OrderedQueue(Consumer<Map<String, Object>> onDeque) {
        this.onDeque = onDeque;
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleWithFixedDelay(() -> {
            currentTimeMillis.set(System.currentTimeMillis());
            // 这里有并发问题，错误的进入了第一个分支
            if (updateWatermarkTimestamp != 0L && currentTimeMillis.get() > updateWatermarkTimestamp + maxFlushAllIntervalMillis) {
                // flush all
                onEventTime(maxTimestamp + maxRetainedMillis);
            } else {
                onEventTime(maxTimestamp);
            }
        }, 1000, 200, TimeUnit.MILLISECONDS);
        Runtime.getRuntime().addShutdownHook(new Thread(service::shutdown));
    }

    public static OrderedQueue onEventDeque(Consumer<Map<String, Object>> action) {
        return new OrderedQueue(action);
    }

    public long getQueueSizeCnt() {
        return queueSizeCnt.get();
    }

    public void bufferEvent(Map<String, Object> element, long timestamp) {
        while (queueSizeCnt.get() > maxQueueSize) {
            // spin lock waiting...
        }
        // 1. queue event
        if (elementQueue.containsKey(timestamp)) {
            elementQueue.get(timestamp).add(element);
        } else {
            List<Map<String, Object>> queue = new ArrayList<>();
            queue.add(element);
            elementQueue.put(timestamp, queue);
        }
        queueSizeCnt.incrementAndGet();

        // 2. set current watermark
        if (timestamp >= maxTimestamp) {
            maxTimestamp = timestamp;
            updateWatermarkTimestamp = currentTimeMillis.get();
        }
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
            if (timestamp < lastOutTimestamp) {
                System.out.println("Out of order after queue: " + timestamp + ", " + lastOutTimestamp);
            }
            lastOutTimestamp = timestamp;
            List<Map<String, Object>> queue = elementQueue.remove(timestamp);
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
