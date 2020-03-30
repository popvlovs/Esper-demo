package espercep.demo.cases;

import com.lmax.disruptor.*;
import com.lmax.disruptor.util.DaemonThreadFactory;
import espercep.demo.util.MetricUtil;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.lmax.disruptor.RingBuffer.createSingleProducer;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class DisruptorMetric_2 {

    private static final int NUM_PROCESSORS = Runtime.getRuntime().availableProcessors();
    private static final ExecutorService executor = Executors.newFixedThreadPool(NUM_PROCESSORS, DaemonThreadFactory.INSTANCE);

    public static void main(String[] args) throws Exception {
        EventFactory<LongEventBeanWrapper> eventFactory = new EventFactory<LongEventBeanWrapper>() {
            @Override
            public LongEventBeanWrapper newInstance() {
                return new LongEventBeanWrapper();
            }
        };

        final RingBuffer<LongEventBeanWrapper> ringBuffer = createSingleProducer(eventFactory, 2 << 13, new YieldingWaitStrategy());
        final SequenceBarrier sequenceBarrier = ringBuffer.newBarrier();
        final EventHandler[] handlers = new EventHandler[NUM_PROCESSORS];
        final BatchEventProcessor<?>[] batchEventProcessors = new BatchEventProcessor[NUM_PROCESSORS];
        for (int i = 0; i < NUM_PROCESSORS; i++) {
            handlers[i] = new Consumer(i);
            batchEventProcessors[i] = new BatchEventProcessor<LongEventBeanWrapper>(ringBuffer, sequenceBarrier, handlers[i]);
            ringBuffer.addGatingSequences(batchEventProcessors[i].getSequence());
            executor.submit(batchEventProcessors[i]);
        }

        // 3. 持续生成
        sendRandomEvents(ringBuffer);
    }

    private static void sendRandomEvents(RingBuffer<LongEventBeanWrapper> buffer) {
        long remainingEvents = Long.MAX_VALUE;
        while (--remainingEvents > 0) {
            MetricUtil.getConsumeRateMetric().mark();

            long sequence = buffer.next();
            buffer.get(sequence).setValue(remainingEvents);
            buffer.publish(sequence);
        }
    }

    private static class MapEventBeanWrapper {
        private Map<String, Object> innerMap;

        public MapEventBeanWrapper() {
        }

        public MapEventBeanWrapper(Map<String, Object> innerMap) {
            this.innerMap = innerMap;
        }

        public void setInnerMap(Map<String, Object> innerMap) {
            this.innerMap = innerMap;
        }

        public Map<String, Object> getInnerMap() {
            return innerMap;
        }
    }

    private static class LongEventBeanWrapper {
        private long value;

        public long getValue() {
            return value;
        }

        public void setValue(long value) {
            this.value = value;
        }
    }

    public static class Consumer implements EventHandler<LongEventBeanWrapper> {
        private int consumerId;

        public Consumer(int consumerId) {
            this.consumerId = consumerId;
        }

        @Override
        public void onEvent(LongEventBeanWrapper map, long sequence, boolean endOfBatch) throws Exception {
            MetricUtil.getCounter("total consumed engine#" + this.consumerId).inc();
            // engine.sendEvent(map.getInnerMap(), "TestEvent");
        }
    }
}
