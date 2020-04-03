package espercep.demo.cases.esper;

import com.alibaba.fastjson.JSONObject;
import com.espertech.esper.client.*;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
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
public class FollowByMetric_Multithread_6 {

    private static final Logger logger = LoggerFactory.getLogger(FollowByMetric_Multithread_6.class);

    public static void main(String[] args) throws Exception {
        // Set event representation
        Configuration configuration = new Configuration();

        // Multi-thread may cause detection missing
        // configuration.getEngineDefaults().getThreading().setThreadPoolInbound(true);
        // configuration.getEngineDefaults().getThreading().setThreadPoolInboundCapacity(1000);
        // configuration.getEngineDefaults().getThreading().setThreadPoolInboundNumThreads(Runtime.getRuntime().availableProcessors());

        //configuration.getEngineDefaults().getExecution().setDisableLocking(true);

        String epl = FileUtil.readResourceAsString("epl_case7_follow_by_100.sql");

        // 1. 按 cpu 核数初始化esper engine线程
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
                            MetricUtil.getCounter("Detected patterns " + eplName).inc();
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

        // 2. 创建 Disruptor 用于线程间通信
        // Send event to consumer thread
        Disruptor<Map> disruptor = new Disruptor<>(new EventFactory<Map>() {
            @Override
            public Map newInstance() {
                return new HashMap<String, Object>();
            }
        }, 2 << 18, Executors.defaultThreadFactory(), ProducerType.SINGLE, new BlockingWaitStrategy());
        disruptor.handleEventsWith(workerHandlers.toArray(new Consumer[]{}));
        disruptor.start();

        // 3. 持续生成
        sendRandomEvents(disruptor);
    }

    private static void sendRandomEvents(Disruptor disruptor) {
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

            disruptor.publishEvent(new EventTranslator<Map>() {
                @Override
                public void translateTo(Map map, long sequence) {
                    map.putAll(element);
                }
            });
        }
    }

    private static class Consumer implements EventHandler<Map> {
        private int consumerId;
        private EPRuntime engine;

        public Consumer(int consumerId, EPRuntime engine) {
            this.consumerId = consumerId;
            this.engine = engine;
        }

        @Override
        public void onEvent(Map map, long sequence, boolean endOfBatch) throws Exception {
            MetricUtil.getCounter("total consumed engine#"+this.consumerId).inc();
            engine.sendEvent(map, "TestEvent");
        }
    }
}
