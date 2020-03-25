package espercep.demo.cases;

import com.alibaba.fastjson.JSONObject;
import com.espertech.esper.client.*;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import espercep.demo.udf.UserDefinedFunction;
import espercep.demo.util.FileUtil;
import espercep.demo.util.MetricUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * 使用原生win:ext_timed_batch实现的demo
 *
 * @author yitian_song
 */
public class WindowCountMetric_Multithread_6 {
    private static final Logger logger = LoggerFactory.getLogger(WindowCountMetric_Multithread_6.class);

    private static final int GROUP_BY_FIELD_NUM = 3;
    private static final int RULE_NUM = 50;
    private static final int GROUP_BY_DIVERSITY_DEGREE = 16;

    public static void main(String[] args) throws Exception {
        // Set event representation
        Configuration configuration = new Configuration();

        // Multi-thread may cause detection missing
        //configuration.getEngineDefaults().getThreading().setThreadPoolInbound(true);
        //configuration.getEngineDefaults().getThreading().setThreadPoolInboundCapacity(1000);
        //configuration.getEngineDefaults().getThreading().setThreadPoolInboundNumThreads(Runtime.getRuntime().availableProcessors());

        // Define UDF
        configuration.addPlugInSingleRowFunction("win_count", UserDefinedFunction.class.getName(), "winCount", ConfigurationPlugInSingleRowFunction.ValueCache.ENABLED);
        configuration.addPlugInSingleRowFunction("timer_terminate", UserDefinedFunction.class.getName(), "timerTerminate", ConfigurationPlugInSingleRowFunction.ValueCache.ENABLED);
        configuration.addPlugInSingleRowFunction("first_occur_time", UserDefinedFunction.class.getName(), "firstOccurTime", ConfigurationPlugInSingleRowFunction.ValueCache.ENABLED);
        configuration.addPlugInSingleRowFunction("last_occur_time", UserDefinedFunction.class.getName(), "lastOccurTime", ConfigurationPlugInSingleRowFunction.ValueCache.ENABLED);

        int cpuCores = Runtime.getRuntime().availableProcessors();
        final List<Consumer> workerHandlers = new ArrayList<>(cpuCores);
        AtomicInteger numEpl = new AtomicInteger(RULE_NUM);

        for (int engineIndex = 0; engineIndex < cpuCores; ++engineIndex) {
            try {
                EPServiceProvider epService = EPServiceProviderManager.getProvider("esper#" + engineIndex, configuration);
                Map<String, Object> eventType = new HashMap<>();
                eventType.put("event_name", String.class);
                eventType.put("event_id", Long.class);
                for (int k = 0; k < GROUP_BY_FIELD_NUM; k++) {
                    eventType.put("group_" + k, Integer.class);
                }
                eventType.put("src_address", String.class);
                eventType.put("dst_address", String.class);
                eventType.put("occur_time", Long.class);
                epService.getEPAdministrator().getConfiguration().addEventType("TestEvent", eventType);

                boolean isAnyEplRunning = false;
                for (int ruleIndex = 0; ruleIndex < numEpl.get(); ruleIndex++) {
                    if (ruleIndex % cpuCores != engineIndex) {
                        continue;
                    }
                    isAnyEplRunning = true;
                    String epl = getRealEPL(FileUtil.readResourceAsString("epl_case15_count_window.sql"));
                    logger.info("create EPL: {}", epl);
                    createStatement(epService, epl, engineIndex, ruleIndex);
                }
                if (isAnyEplRunning) {
                    workerHandlers.add(new Consumer(engineIndex, epService.getEPRuntime()));
                }
            } catch (Exception e) {
                throw new RuntimeException("Error on execute eql", e);
            }
        }

        sendEventsThroughDisruptor(workerHandlers);
    }

    private static String getRealEPL(String epl) {
        List<String> groupByKeys = new ArrayList<>();
        for (int i = 0; i < GROUP_BY_FIELD_NUM; i++) {
            groupByKeys.add("A.group_" + i);
        }
        String groupBy = groupByKeys.stream().reduce((lhs, rhs) -> lhs + "," + rhs).orElse("");
        return MessageFormat.format(epl, groupBy);
    }

    private static EPStatement createStatement(EPServiceProvider epService, String epl, int engineIdx, int ruleIdx) {
        final String ruleName = "CountWindow#" + ruleIdx;
        EPStatement epStatement = epService.getEPAdministrator().createEPL(epl, ruleName);
        epStatement.addListener((newData, oldData, stat, rt) -> {
            Arrays.stream(newData).forEach(data -> {
                Map result = (Map) data.getUnderlying();
                String countStr = Optional.ofNullable(result.get("win_count")).orElse("0").toString();
                int count = Integer.parseInt(countStr);
                if (count > 0) {
                    String eventName = result.get("event_name").toString();
                    MetricUtil.getCounter(String.format("Engine#%d, rule#%d, %s output total", engineIdx, ruleIdx, eventName)).inc(count);
                    MetricUtil.getCounter(String.format("Engine#%d, rule#%d, %s output times", engineIdx, ruleIdx, eventName)).inc();
                }
            });
            MetricUtil.getCounter("Detected patterns ").inc();
        });
        return epStatement;
    }

    private static void sendEventsThroughDisruptor(List<Consumer> workerHandlers) {
        Disruptor<MapEventBeanWrapper> disruptor = new Disruptor<>(new EventFactory<MapEventBeanWrapper>() {
            @Override
            public MapEventBeanWrapper newInstance() {
                return new MapEventBeanWrapper();
            }
        }, 2 << 18, Executors.defaultThreadFactory(), ProducerType.SINGLE, new YieldingWaitStrategy());
        disruptor.handleEventsWith(workerHandlers.toArray(new Consumer[]{}));
        disruptor.start();

        sendRandomEvents(disruptor);
    }

    private static void sendRandomEvents(Disruptor<MapEventBeanWrapper> disruptor) {
        long now = System.currentTimeMillis();
        long remainingEvents = Long.MAX_VALUE;
        long cnt = 0;
        String[] eventNames = new String[]{"A", "B", "C"};
        while (--remainingEvents > 0) {
            int randomVal = new Random().nextInt(eventNames.length);
            String eventName = eventNames[randomVal % eventNames.length];
            JSONObject element = new JSONObject();
            element.put("event_id", cnt++);
            for (int i = 0; i < GROUP_BY_FIELD_NUM; i++) {
                element.put("group_" + i, new Random().nextInt(GROUP_BY_DIVERSITY_DEGREE));
            }

            element.put("event_name", eventName);
            element.put("src_address", "172.16.100." + cnt % 0xFF);
            element.put("dst_address", "172.16.100." + cnt % 0xFF);
            //element.put("occur_time", Long.MAX_VALUE - remainingEvents + now);
            element.put("occur_time", System.currentTimeMillis());
            MetricUtil.getConsumeRateMetric().mark();
            MetricUtil.getCounter(eventName + " inputs").inc();
            disruptor.publishEvent(new EventTranslator<MapEventBeanWrapper>() {
                @Override
                public void translateTo(MapEventBeanWrapper map, long sequence) {
                    map.setInnerMap(element);
                }
            });
        }
    }

    private static class MapEventBeanWrapper {
        Map<String, Object> innerMap;

        public MapEventBeanWrapper() {
            this.innerMap = null;
        }

        public MapEventBeanWrapper(Map<String, Object> innerMap) {
            this.innerMap = innerMap;
        }

        public void copyTo(MapEventBeanWrapper rhs) {
            MapEventBeanWrapper lhs = this;
            rhs.innerMap = lhs.innerMap;
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

        public Consumer(int consumerId, EPRuntime engine) {
            this.consumerId = consumerId;
            this.engine = engine;
        }

        @Override
        public void onEvent(MapEventBeanWrapper map, long sequence, boolean endOfBatch) throws Exception {
            MetricUtil.getCounter("total consumed engine#" + this.consumerId).inc();
            engine.sendEvent(map.getInnerMap(), "TestEvent");
        }
    }
}
