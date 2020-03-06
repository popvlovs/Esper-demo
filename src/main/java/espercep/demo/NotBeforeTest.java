package espercep.demo;

import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.espertech.esper.client.*;
import espercep.demo.udf.UserDefinedFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class NotBeforeTest {
    public static void main(String[] args) {
        // Set event representation
        Configuration configuration = new Configuration();

        // Multi-thread
        /*configuration.getEngineDefaults().getThreading().setThreadPoolInbound(true);
        configuration.getEngineDefaults().getThreading().setThreadPoolInboundCapacity(1000);
        configuration.getEngineDefaults().getThreading().setThreadPoolInboundNumThreads(Runtime.getRuntime().availableProcessors());*/

        configuration.addPlugInSingleRowFunction("UPDATE_LAST_A", UserDefinedFunction.class.getName(), "updateLastA", ConfigurationPlugInSingleRowFunction.ValueCache.ENABLED);
        configuration.addPlugInSingleRowFunction("NOT_BEFORE", UserDefinedFunction.class.getName(), "notBefore", ConfigurationPlugInSingleRowFunction.ValueCache.ENABLED);
        configuration.addPlugInSingleRowFunction("setActiveTime", UserDefinedFunction.class.getName(), "setActiveTime", ConfigurationPlugInSingleRowFunction.ValueCache.ENABLED);

        EPServiceProvider epService = EPServiceProviderManager.getProvider("esper", configuration);
        Map<String, Object> eventType = new HashMap<>();
        eventType.put("event_name", String.class);
        eventType.put("event_id", Long.class);
        eventType.put("src_address", String.class);
        eventType.put("dst_address", String.class);
        eventType.put("occur_time", Long.class);
        epService.getEPAdministrator().getConfiguration().addEventType("TestEvent", eventType);

        try {
            String epl = "select * from pattern[every (A=TestEvent(setActiveTime('rule_0001', A) and (event_name = 'A') and UPDATE_LAST_A('rule_0001', A)) or B=TestEvent(event_name = 'B' and NOT_BEFORE('rule_0001', B, 10000)))]";
            String name = "Custom not-before";
            String nameWithNum = name + "#" + 1;
            EPStatement epStatement = epService.getEPAdministrator().createEPL(epl, nameWithNum);
            epStatement.addListener((newData, oldData, stat, rt) -> {
                System.out.println("[" + nameWithNum + "] selected row: " + JSONObject.toJSONString(newData[0].getUnderlying()));
            });
            // Send events
            sendEvents(epService.getEPRuntime());
        } catch (Exception e) {
            throw new RuntimeException("Error on execute eql", e);
        }
    }

    private static Meter consumeRate;

    static {
        final MetricRegistry metrics = new MetricRegistry();
        consumeRate = metrics.meter("Produce rate");
        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(1, TimeUnit.SECONDS);
    }

    private static void sendRandomEvents(EPRuntime epRuntime) {
        long remainingEvents = 10_000_000;
        long cnt = 0;
        String[] eventNames = new String[]{"A", "B", "C"};
        while (--remainingEvents > 0) {
            int randomVal = new Random().nextInt(3);
            JSONObject element = new JSONObject();
            element.put("event_id", cnt++);
            element.put("event_name", eventNames[randomVal % 3]);
            element.put("src_address", "172.16.100." + cnt % 0xFF);
            element.put("dst_address", "172.16.100." + cnt % 0xFF);
            element.put("occur_time", System.currentTimeMillis() + ((cnt%10000 == 0 && randomVal%3 == 1) ? 1000L : 0L));
            consumeRate.mark();
            epRuntime.sendEvent(element, "TestEvent");
        }
    }

    private static void sendEvents(EPRuntime esperRuntime) throws InterruptedException {
        Long basetime = System.currentTimeMillis();
        int eventId = 0;

        // Send event B
        JSONObject elementB2 = new JSONObject();
        elementB2.put("event_id", eventId++);
        elementB2.put("event_name", "B");
        elementB2.put("src_address", "172.16.101.1");
        elementB2.put("dst_address", "172.16.100.0");
        elementB2.put("occur_time", basetime + 3200L);
        esperRuntime.sendEvent(elementB2, "TestEvent");


        // Send event B
        JSONObject elementB1 = new JSONObject();
        elementB1.put("event_id", eventId++);
        elementB1.put("event_name", "B");
        elementB1.put("src_address", "172.16.101.1");
        elementB1.put("dst_address", "172.16.100.0");
        elementB1.put("occur_time", basetime + 13300L);
        esperRuntime.sendEvent(elementB1, "TestEvent");
    }
}
