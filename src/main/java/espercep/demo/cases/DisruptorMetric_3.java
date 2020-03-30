package espercep.demo.cases;

import com.alibaba.fastjson.JSONObject;
import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import espercep.demo.util.ArgsUtil;
import espercep.demo.util.CmdLineOptions;
import espercep.demo.util.FileUtil;
import espercep.demo.util.MetricUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class DisruptorMetric_3 {

    private static final Logger logger = LoggerFactory.getLogger(DisruptorMetric_3.class);
    private static CmdLineOptions options;

    public static void main(String[] args) throws Exception {
        options = ArgsUtil.getArg(args);
        logger.info("Using args as {}", options);

        // 1. 按 cpu 核数初始化esper engine线程

        final List<Consumer> workerHandlers = new ArrayList<>(options.getCoreNum());
        for (int engineIndex = 0; engineIndex < options.getCoreNum(); ++engineIndex) {
            try {
                workerHandlers.add(new Consumer(engineIndex));
            } catch (Exception e) {
                throw new RuntimeException("Error on execute eql", e);
            }
        }

        // 2. 创建 Disruptor 用于线程间通信
        // Send event to consumer thread
        Disruptor<MapEventBeanWrapper> disruptor = new Disruptor<>(new EventFactory<MapEventBeanWrapper>() {
            @Override
            public MapEventBeanWrapper newInstance() {
                return new MapEventBeanWrapper();
            }
        }, options.getRingBufferSize(), options.getThreadFactory(), ProducerType.SINGLE, options.getWaitingStrategy());
        disruptor.handleEventsWith(workerHandlers.toArray(new Consumer[]{}));
        disruptor.start();

        // 3. 持续生成
        sendRandomEvents(disruptor);
    }

    private static void sendRandomEvents(Disruptor<MapEventBeanWrapper> disruptor) {
        long remainingEvents = Long.MAX_VALUE;
        long cnt = 0;
        String[] eventNames = new String[]{"A", "B", "C"};
        while (--remainingEvents > 0) {
            int randomVal = new Random().nextInt(eventNames.length);
            /*String eventName = eventNames[randomVal % eventNames.length];
            JSONObject element = new JSONObject();
            element.put("event_id", cnt++);
            for (int i = 0; i < options.getGroupByNum(); i++) {
                element.put("group_" + i, new Random().nextInt(options.getGroupByComplexity()));
            }

            element.put("event_name", eventName);
            element.put("src_address", "172.16.100." + cnt % 0xFF);
            element.put("dst_address", "172.16.100." + cnt % 0xFF);
            //element.put("occur_time", Long.MAX_VALUE - remainingEvents + now);
            element.put("occur_time", System.currentTimeMillis());
            MetricUtil.getCounter(eventName + " inputs").inc();*/
            MetricUtil.getConsumeRateMetric().mark();
            disruptor.publishEvent(new EventTranslator<MapEventBeanWrapper>() {
                @Override
                public void translateTo(MapEventBeanWrapper map, long sequence) {
                    map.setInnerMap(new HashMap<>());
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

        public void setInnerMap(Map<String, Object> innerMap) {
            this.innerMap = innerMap;
        }

        public Map<String, Object> getInnerMap() {
            return innerMap;
        }
    }

    public static class Consumer implements EventHandler<MapEventBeanWrapper> {
        private int consumerId;

        public Consumer(int consumerId) {
            this.consumerId = consumerId;
        }

        @Override
        public void onEvent(MapEventBeanWrapper map, long sequence, boolean endOfBatch) throws Exception {
            MetricUtil.getCounter("total consumed engine#" + this.consumerId).inc();
        }
    }
}
