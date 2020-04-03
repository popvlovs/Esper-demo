package espercep.demo.cases.esper;

import com.alibaba.fastjson.JSONObject;
import com.espertech.esper.client.*;
import com.lmax.disruptor.*;
import espercep.demo.util.FileUtil;
import espercep.demo.util.MetricUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class FollowByMetric_Multithread_5 {

    private static final Logger logger = LoggerFactory.getLogger(FollowByMetric_Multithread_5.class);

    public static void main(String[] args) throws Exception {
        // Set event representation
        Configuration configuration = new Configuration();

        // Multi-thread may cause detection missing
        // configuration.getEngineDefaults().getThreading().setThreadPoolInbound(true);
        // configuration.getEngineDefaults().getThreading().setThreadPoolInboundCapacity(1000);
        // configuration.getEngineDefaults().getThreading().setThreadPoolInboundNumThreads(Runtime.getRuntime().availableProcessors());

        //configuration.getEngineDefaults().getExecution().setDisableLocking(true);

        String epl = FileUtil.readResourceAsString("epl_case7_follow_by_100.sql");

        int cpuCores = Runtime.getRuntime().availableProcessors();
        CountDownLatch cdt = new CountDownLatch(cpuCores);

        final List<Consumer> workerHandlers = new ArrayList<>(cpuCores);

        ExecutorService executorService = Executors.newFixedThreadPool(cpuCores);
        int numTaskPerCore = (0xFF+1) / cpuCores;
        for (int i = 0; i < cpuCores; ++i) {
            final int engineIndex = i;
            executorService.submit(() -> {
                EPServiceProvider epService = EPServiceProviderManager.getProvider("esper#" + engineIndex, configuration);
                Map<String, Object> eventType = new HashMap<>();
                eventType.put("event_name", String.class);
                eventType.put("event_id", Long.class);
                eventType.put("src_address", String.class);
                eventType.put("dst_address", String.class);
                eventType.put("occur_time", Long.class);
                epService.getEPAdministrator().getConfiguration().addEventType("TestEvent", eventType);

                try {
                    for (int j = engineIndex * numTaskPerCore; j < (engineIndex + 1) * numTaskPerCore; ++j) {
                        String regularEpl = MessageFormat.format(epl, j, j);
                        final String eplName = "Engine#" + j;
                        EPStatement epStatement = epService.getEPAdministrator().createEPL(regularEpl, eplName);
                        epStatement.addListener((newData, oldData, stat, rt) -> {
                            MetricUtil.getCounter("Detected patterns ").inc();
                            //System.out.println("selected row: " + JSONObject.toJSONString(newData[0].getUnderlying()));
                        });
                    }
                    workerHandlers.add(new Consumer(engineIndex, epService.getEPRuntime()));
                    cdt.countDown();
                } catch (Exception e) {
                    throw new RuntimeException("Error on execute eql", e);
                }
            });
        }
        cdt.await();

        // Send event to consumer thread
        RingBuffer<Map> sharedBuffer = RingBuffer.createSingleProducer(new EventFactory<Map>() {
            @Override
            public Map newInstance() {
                return new HashMap<String, Object>();
            }
        }, 2<<18, new YieldingWaitStrategy());
        SequenceBarrier barrier = sharedBuffer.newBarrier();
        WorkerPool<Map> workerPool = new WorkerPool<>(sharedBuffer, barrier, new ExceptionHandler<Map>() {
            @Override
            public void handleEventException(Throwable throwable, long timestamp, Map map) {
                logger.error("Error on shared buffer handle event: {}, {}", timestamp, JSONObject.toJSONString(map), throwable);
            }

            @Override
            public void handleOnStartException(Throwable throwable) {
                logger.error("Error on shared buffer start: ", throwable);
            }

            @Override
            public void handleOnShutdownException(Throwable throwable) {
                logger.error("Error on shared buffer shutdown: ", throwable);
            }
        }, workerHandlers.toArray(new Consumer[]{}));
        sharedBuffer.addGatingSequences(workerPool.getWorkerSequences());
        workerPool.start(Executors.newFixedThreadPool(cpuCores));
        sendRandomEvents(sharedBuffer);
    }

    private static void sendRandomEvents(RingBuffer sharedBuffer) {
        final Producer producer = new Producer(sharedBuffer);

        long remainingEvents = Long.MAX_VALUE;
        long cnt = 0;
        String[] eventNames = new String[]{"A", "B"};
        while (--remainingEvents > 0) {
            int randomVal = new Random().nextInt(eventNames.length);
            JSONObject element = new JSONObject();
            element.put("event_id", cnt++);
            element.put("event_name", eventNames[randomVal % eventNames.length]);
            element.put("src_address", "172.16.100." + cnt % 0xFF);
            element.put("dst_address", "172.16.100." + cnt % 0xFF);
            element.put("occur_time", System.currentTimeMillis() + randomVal);
            MetricUtil.getConsumeRateMetric().mark();

            producer.sendData(element);
        }
    }

    private static class Producer {
        private RingBuffer<Map> ringBuffer;

        public Producer(RingBuffer ringBuffer) {
            this.ringBuffer = ringBuffer;
        }

        public void sendData(Map<String, Object> data) {
            long sequence = ringBuffer.next();
            try {
                Map cursor = ringBuffer.get(sequence);
                // TODO warp event
                cursor.putAll(data);
            } finally {
                ringBuffer.publish(sequence);
            }
        }
    }

    private static class Consumer implements WorkHandler<Map> {
        private final Logger logger = LoggerFactory.getLogger(Consumer.class);

        private int consumerId;
        private EPRuntime engine;

        public Consumer(int consumerId, EPRuntime engine) {
            this.consumerId = consumerId;
            this.engine = engine;
        }

        @Override
        public void onEvent(Map map) throws Exception {
            MetricUtil.getCounter("total consumed").inc();
            engine.sendEvent(map, "TestEvent");
        }
    }
}
