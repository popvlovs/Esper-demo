package espercep.demo.cases.esper;

import com.alibaba.fastjson.JSONObject;
import com.espertech.esper.client.*;
import espercep.demo.util.FileUtil;
import espercep.demo.util.MetricUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/3/6
 */
public class MatchRecognize_1 {
    private static final Logger logger = LoggerFactory.getLogger(MatchRecognize_1.class);

    public static void main(String[] args) throws Exception {
        // Set event representation
        Configuration configuration = new Configuration();

        // Multi-thread may cause detection missing
        //configuration.getEngineDefaults().getThreading().setThreadPoolInbound(true);
        //configuration.getEngineDefaults().getThreading().setThreadPoolInboundCapacity(1000);
        //configuration.getEngineDefaults().getThreading().setThreadPoolInboundNumThreads(Runtime.getRuntime().availableProcessors());

        EPServiceProvider epService = EPServiceProviderManager.getProvider("esper", configuration);
        Map<String, Object> eventType = new HashMap<>();
        eventType.put("event_name", String.class);
        eventType.put("event_id", Long.class);
        eventType.put("src_address", String.class);
        eventType.put("dst_address", String.class);
        eventType.put("occur_time", Long.class);
        epService.getEPAdministrator().getConfiguration().addEventType("TestEvent", eventType);

        try {
            String epl = FileUtil.readResourceAsString("eql_case2_permute.sql");
            for (int i = 0; i < 1; ++i) {
                final int index = i;
                String regularEpl = MessageFormat.format(epl, i, i, i);
                logger.info("EPL#" + i + ": {}", regularEpl);
                EPStatement epStatement = epService.getEPAdministrator().createEPL(regularEpl, "EPL#" + i);
                epStatement.addListener((newData, oldData, stat, rt) -> {
                    /*System.out.println("selected row: " + JSONObject.toJSONString(newData[0].getUnderlying()));*/
                    MetricUtil.getCounter("Detected patterns #" + index).inc();
                });
            }

            // Send events
            sendRandomEvents(epService.getEPRuntime());
        } catch (
                Exception e) {
            throw new RuntimeException("Error on execute eql", e);
        }
    }

    private static void sendRandomEvents(EPRuntime epRuntime) {
        long remainingEvents = 10_000_000;
        long cnt = 0;
        String[] eventNames = new String[]{"A", "B", "C", "D", "E"};
        while (--remainingEvents > 0) {
            int randomVal = new Random().nextInt(eventNames.length);
            JSONObject element = new JSONObject();
            element.put("event_id", cnt++);
            element.put("event_name", eventNames[randomVal % eventNames.length]);
            element.put("src_address", "172.16.100." + cnt % 0xFF);
            element.put("dst_address", "172.16.100.1");
            element.put("occur_time", System.currentTimeMillis() + randomVal);
            MetricUtil.getConsumeRateMetric().mark();
            epRuntime.sendEvent(element, "TestEvent");
        }
    }

    private static void sendEvents(EPRuntime esperRuntime) {
        long baseTime = System.currentTimeMillis();
        int eventId = 0;

        // Send event B
        JSONObject elementB2 = new JSONObject();
        elementB2.put("event_id", eventId++);
        elementB2.put("event_name", "B1");
        elementB2.put("src_address", "127.0.0.2");
        elementB2.put("dst_address", "172.16.101.1");
        elementB2.put("occur_time", baseTime + 200L);
        esperRuntime.sendEvent(elementB2, "TestEvent");

        // Send event A
        JSONObject elementA0 = new JSONObject();
        elementA0.put("event_id", eventId++);
        elementA0.put("event_name", "A1");
        elementA0.put("src_address", "127.0.0.2");
        elementA0.put("dst_address", "127.0.0.1");
        elementA0.put("occur_time", baseTime + 100L);
        esperRuntime.sendEvent(elementA0, "TestEvent");
    }
}
