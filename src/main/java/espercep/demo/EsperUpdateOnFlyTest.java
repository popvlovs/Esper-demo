package espercep.demo;

import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.espertech.esper.client.*;
import espercep.demo.udf.UserDefinedFunction;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/7/9
 */
public class EsperUpdateOnFlyTest {
    public static void main(String[] args) {
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
            long now = System.currentTimeMillis();
            String epl = String.format("select * from TestEvent((event_name = 'A') and occur_time <= %d)", now + TimeUnit.SECONDS.toMillis(1));
            String name = "Custom online updating";
            EPStatement epStatement = epService.getEPAdministrator().createEPL(epl, name);
            epStatement.addListener((newData, oldData, stat, rt) -> {
                long occurTimeCond = Arrays.stream(newData)
                        .map(EventBean::getUnderlying)
                        .map(item -> (JSONObject) item)
                        .map(item -> item.getLongValue("occur_time"))
                        .max(Long::compareTo)
                        .orElse(0L);
                occurTimeCond = occurTimeCond + TimeUnit.SECONDS.toMillis(1);
                //System.out.println("output: " + JSONObject.toJSONString(newData[0].getUnderlying()));
                EPStatement statement = epService.getEPAdministrator().getStatement(name);
                Iterator<StatementAwareUpdateListener> listenerIterator = statement.getStatementAwareListeners();
                if (listenerIterator.hasNext()) {
                    String newEpl = epl.replaceAll("occur_time <= [0-9]+", "occur_time <= " + occurTimeCond);
                    statement.destroy();
                    epService.getEPAdministrator().createEPL(newEpl, name).addListener(listenerIterator.next());
                    // statement.stop();
                }
                outputRate.mark();
            });
            // Send events
            sendRandomEvents(epService.getEPRuntime(), now);
        } catch (Exception e) {
            throw new RuntimeException("Error on execute eql", e);
        }
    }

    private static Meter consumeRate;
    private static Meter outputRate;

    static {
        final MetricRegistry metrics = new MetricRegistry();
        consumeRate = metrics.meter("Produce rate");
        outputRate = metrics.meter("Outputs");
        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(1, TimeUnit.SECONDS);
    }

    private static void sendRandomEventsParallel(EPRuntime epRuntime, long now, int parallelism) {
        ExecutorService executorService = Executors.newFixedThreadPool(parallelism);
        for (int i = 0; i < parallelism; i++) {
            executorService.submit(() -> EsperUpdateOnFlyTest.sendRandomEvents(epRuntime, now));
        }
    }

    private static void sendRandomEvents(EPRuntime epRuntime, long now) {
        long remainingEvents = 10_000_000;
        long cnt = 0;
        while (--remainingEvents > 0) {
            JSONObject element = new JSONObject();
            element.put("event_id", cnt++);
            element.put("event_name", "A");
            element.put("src_address", "172.16.100." + cnt % 0xFF);
            element.put("dst_address", "172.16.100." + cnt % 0xFF);
            element.put("occur_time", now += TimeUnit.MILLISECONDS.toMillis(500));
            consumeRate.mark();
            epRuntime.sendEvent(element, "TestEvent");
        }
    }
}
