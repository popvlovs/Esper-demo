package espercep.demo.cases.esper;

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

import java.util.*;
import java.util.concurrent.Executors;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class WindowCountMetric_Multithread_3 {
    private static final Logger logger = LoggerFactory.getLogger(WindowCountMetric_Multithread_3.class);

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

        EPServiceProvider epService = EPServiceProviderManager.getProvider("esper", configuration);
        Map<String, Object> eventType = new HashMap<>();
        eventType.put("event_name", String.class);
        eventType.put("event_id", Long.class);
        for (int i = 0; i < 10; i++) {
            eventType.put("group_"+i, Integer.class);
        }
        eventType.put("src_address", String.class);
        eventType.put("dst_address", String.class);
        eventType.put("occur_time", Long.class);
        epService.getEPAdministrator().getConfiguration().addEventType("TestEvent", eventType);


        try {
            String epl = FileUtil.readResourceAsString("epl_case11_count_window.sql");
            EPStatement epStatement = epService.getEPAdministrator().createEPL(epl, "CountWindow#1");
            epStatement.addListener((newData, oldData, stat, rt) -> {
                Arrays.stream(newData).forEach(data -> {
                    Map result = (Map) data.getUnderlying();
                    String countStr = Optional.ofNullable(result.get("win_count")).orElse("0").toString();
                    MetricUtil.getCounter(result.get("event_name").toString() + " outputs").inc(Integer.parseInt(countStr));
                    logger.info(JSONObject.toJSONString(result));
                });
                MetricUtil.getCounter("Detected patterns ").inc();
            });

            sendEventsThroughDisruptor(epService.getEPRuntime());
        } catch (Exception e) {
            throw new RuntimeException("Error on execute eql", e);
        }
    }

    private static void sendEventsThroughDisruptor(EPRuntime epRuntime) {
        Disruptor<MapEventBeanWrapper> disruptor = new Disruptor<>(new EventFactory<MapEventBeanWrapper>() {
            @Override
            public MapEventBeanWrapper newInstance() {
                return new MapEventBeanWrapper();
            }
        }, 2 << 18, Executors.defaultThreadFactory(), ProducerType.SINGLE, new YieldingWaitStrategy());
        disruptor.handleEventsWith(new Consumer(0, epRuntime));
        disruptor.start();

        sendRandomEvents(disruptor);
    }

    private static void sendRandomEvents(Disruptor<MapEventBeanWrapper> disruptor) {
        long now = System.currentTimeMillis();
        long remainingEvents = Long.MAX_VALUE;
        long cnt = 0;
        String[] eventNames = new String[]{"A", "B"};
        while (--remainingEvents > 0) {
            int randomVal = new Random().nextInt(eventNames.length);
            String eventName = eventNames[randomVal % eventNames.length];
            JSONObject element = new JSONObject();
            element.put("event_id", cnt++);
            for (int i = 0; i < 10; i++) {
                element.put("group_" + i, new Random().nextInt(eventNames.length));
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
