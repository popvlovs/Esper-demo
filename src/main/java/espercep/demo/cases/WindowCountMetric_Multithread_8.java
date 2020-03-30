package espercep.demo.cases;

import com.alibaba.fastjson.JSONObject;
import com.espertech.esper.client.*;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import espercep.demo.state.CountWindowGroupState;
import espercep.demo.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 * <p>
 * 使用自定义CountWindowGroupState实现count-window的demo
 *
 * @author yitian_song
 */
public class WindowCountMetric_Multithread_8 {
    private static final Logger logger = LoggerFactory.getLogger(WindowCountMetric_Multithread_8.class);
    private static CmdLineOptions options;

    public static void main(String[] args) throws Exception {
        options = ArgsUtil.getArg(args);
        logger.info("Using args as {}", options);

        MetricUtil.disable(options.isNoMetric());

        // Create Esper engine(s)
        Configuration configuration = new Configuration();
        String epl = FileUtil.readResourceAsString("epl_case16_count_window.sql");
        final List<Consumer> workerHandlers = new ArrayList<>(options.getCoreNum());
        for (int engineIndex = 0; engineIndex < options.getCoreNum(); ++engineIndex) {
            try {
                EPServiceProvider epService = EPServiceProviderManager.getProvider("esper#" + engineIndex, configuration);
                Map<String, Object> eventType = new HashMap<>();
                eventType.put("event_name", String.class);
                eventType.put("event_id", Long.class);
                for (int groupIndex = 0; groupIndex < options.getGroupByNum(); groupIndex++) {
                    eventType.put("group_" + groupIndex, Integer.class);
                }
                eventType.put("src_address", String.class);
                eventType.put("dst_address", String.class);
                eventType.put("occur_time", Long.class);
                epService.getEPAdministrator().getConfiguration().addEventType("TestEvent", eventType);

                boolean isAnyEplRunning = false;
                for (int ruleIdx = 0; ruleIdx < options.getRuleNum(); ruleIdx++) {
                    if (ruleIdx % options.getCoreNum() != engineIndex) {
                        continue;
                    }
                    isAnyEplRunning = true;
                    createStatement(epService, epl, engineIndex, ruleIdx);
                }
                if (isAnyEplRunning) {
                    workerHandlers.add(new Consumer(engineIndex, epService.getEPRuntime(), options.getEventNum()));
                }
            } catch (Exception e) {
                throw new RuntimeException("Error on execute eql", e);
            }
        }
        CountDownLatch cdt = new CountDownLatch(workerHandlers.size());
        workerHandlers.forEach(handler -> handler.setCdt(cdt));
        long startTime = System.currentTimeMillis();
        if (options.isNoDisruptor()) {
            ExecutorService service = Executors.newFixedThreadPool(options.getCoreNum(), options.getThreadFactory(options.getAvailableCores()));
            for (Consumer consumer : workerHandlers) {
                service.submit(() -> sendRandomEvents(consumer));
            }
        } else {
            sendEventsThroughDisruptor(workerHandlers);
        }
        cdt.await();
        logger.info("Time cost: {} ms", System.currentTimeMillis() - startTime);
        System.exit(0);
    }

    @SuppressWarnings("unchecked")
    private static EPStatement createStatement(EPServiceProvider epService, String epl, int engineIdx, int ruleIdx) {
        final String ruleName = "CountWindow#" + ruleIdx;
        CountWindowGroupState<Map> stateOfRule = new CountWindowGroupState<>()
                .groupBy(getGroupKeys())
                .outputLastAs("event_id", "last_event_id")
                .outputLastAs("src_address", "last_src_address")
                .outputLastAs("dst_address", "last_dst_address")
                .outputLastAs("occur_time", "last_occur_time")
                .outputLastAs("event_name", "last_event_name")
                .outputWindowAs("event_id", "event_ids")
                .outputWindowAs("src_address", "src_address_arr")
                .outputWindowAs("dst_address", "dst_address_arr")
                .outputWindowAs("occur_time", "occur_time")
                .outputWindowAs("event_name", "event_name")
                .outputWindowAs("group_0", "group_0")
                .outputWindowAs("group_1", "group_1")
                .outputWindowAs("group_2", "group_2")
                .outputCountAs("count");
        EPStatement epStatement = epService.getEPAdministrator().createEPL(epl, ruleName, stateOfRule);
        /*epStatement.addListener((newData, oldData, stat, rt) -> {
            // CountWindowGroupState<Map> state = (CountWindowGroupState<Map>) stat.getUserObject();
            // Inbound
            Arrays.stream(newData).forEach(data -> {
                MetricUtil.getCounter(String.format("Engine #%d, Rule #%s inbound", engineIdx, ruleIdx)).inc();

                *//*Map dataMap = (Map) data.getUnderlying();
                String eventName = dataMap.get("event_name").toString();
                List<Map<String, Object>> result = state.applyEntry(dataMap);
                if (result != null) {
                    result.forEach(output -> {
                        long count = (long) output.get("count");
                        MetricUtil.getCounter(String.format("Engine#%d, rule#%d, %s output total", engineIdx, ruleIdx, eventName)).inc(count);
                        MetricUtil.getCounter(String.format("Engine#%d, rule#%d, %s output times", engineIdx, ruleIdx, eventName)).inc();
                    });
                }*//*
            });
        });*/
        return epStatement;
    }

    private static void sendEventsThroughDisruptor(List<Consumer> workerHandlers) {
        Disruptor<MapEventBeanWrapper> disruptor = new Disruptor<>(new EventFactory<MapEventBeanWrapper>() {
            @Override
            public MapEventBeanWrapper newInstance() {
                return new MapEventBeanWrapper();
            }
        }, options.getRingBufferSize(), options.getThreadFactory(options.getAvailableCores()), ProducerType.SINGLE, options.getWaitingStrategy());
        disruptor.handleEventsWith(workerHandlers.toArray(new Consumer[]{}));
        disruptor.start();

        sendRandomEvents(disruptor);
    }

    private static void sendRandomEvents(Disruptor<MapEventBeanWrapper> disruptor) {
        long remainingEvents = options.getEventNum();

        while (remainingEvents-- > 0) {
            Map<String, Object> element = mock();
            disruptor.publishEvent(new EventTranslator<MapEventBeanWrapper>() {
                @Override
                public void translateTo(MapEventBeanWrapper map, long sequence) {
                    map.setInnerMap(element);
                }
            });
        }
        logger.info("Completed producer");
    }

    private static void sendRandomEvents(Consumer consumer) {
        try {
            long remainingEvents = options.getEventNum();

            while (remainingEvents-- > 0) {
                Map<String, Object> element = mock();
                consumer.onEvent(new MapEventBeanWrapper(element), 0, false);
            }
        } catch (Exception e) {
            logger.error("error: ", e);
        }
    }

    private static ThreadLocal<Long> cnt = ThreadLocal.withInitial(() -> 0L);

    private static Map<String, Object> mock() {
        long localCnt = cnt.get();
        final String[] eventNames = new String[]{"A", "B", "C"};
        String eventName = eventNames[(int) localCnt % eventNames.length];
        JSONObject element = new JSONObject();

        element.put("event_id", localCnt++);
        for (int i = 0; i < 3; i++) {
            element.put("group_" + i, localCnt % 16);
        }
        cnt.set(localCnt);

        element.put("event_name", eventName);
        element.put("src_address", "172.16.100." + localCnt % 0xFF);
        element.put("dst_address", "172.16.100." + localCnt % 0xFF);
        //element.put("occur_time", Long.MAX_VALUE - remainingEvents + now);
        element.put("occur_time", System.currentTimeMillis());
        MetricUtil.getCounter(eventName + " inputs").inc();
        MetricUtil.getConsumeRateMetric().mark();
        return element;
    }

    private static String[] getGroupKeys() {
        String[] keys = new String[options.getGroupByNum()];
        for (int i = 0; i < options.getGroupByNum(); i++) {
            keys[i] = "group_" + i;
        }
        return keys;
    }

    private static class MapEventBeanWrapper {
        Map<String, Object> innerMap;

        public MapEventBeanWrapper() {
            this.innerMap = null;
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

    public static class Consumer implements EventHandler<MapEventBeanWrapper> {
        private int consumerId;
        private EPRuntime engine;
        private long expectedEventNum;
        private CountDownLatch cdt;

        public Consumer(int consumerId, EPRuntime engine, long expectedEventNum) {
            this.consumerId = consumerId;
            this.engine = engine;
            this.expectedEventNum = expectedEventNum;
        }

        public void setCdt(CountDownLatch cdt) {
            this.cdt = cdt;
        }

        @Override
        public void onEvent(MapEventBeanWrapper map, long sequence, boolean endOfBatch) throws Exception {
            MetricUtil.getCounter("total consumed engine#" + this.consumerId).inc();
            if (!options.isNoEsper()) {
                long cpuTimeBefore = TimeMetricUtil.getCPUCurrentThread();
                long wallTimeBefore = TimeMetricUtil.getWall();
                //Map<String, Object> local = new HashMap<>(map.getInnerMap());
                engine.sendEvent(map.getInnerMap(), "TestEvent");
                long cpuTimeCost = TimeMetricUtil.getCPUCurrentThread() - cpuTimeBefore;
                long wallTimeCost = TimeMetricUtil.getWall() - wallTimeBefore;
                MetricUtil.getCounter("engine cpu time nano #" + this.consumerId).inc(cpuTimeCost);
                MetricUtil.getCounter("engine wall time nano #" + this.consumerId).inc(wallTimeCost);
            }
            if (--expectedEventNum <= 0) {
                logger.info("Completed consume #{}", this.consumerId);
                this.cdt.countDown();
            }
        }
    }
}
