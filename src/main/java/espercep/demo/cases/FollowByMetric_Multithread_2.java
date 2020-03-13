package espercep.demo.cases;

import com.alibaba.fastjson.JSONObject;
import com.espertech.esper.client.*;
import espercep.demo.util.FileUtil;
import espercep.demo.util.MetricUtil;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class FollowByMetric_Multithread_2 {
    public static void main(String[] args) throws Exception {
        // Set event representation
        Configuration configuration = new Configuration();

        // Multi-thread may cause detection missing
        // configuration.getEngineDefaults().getThreading().setThreadPoolInbound(true);
        // configuration.getEngineDefaults().getThreading().setThreadPoolInboundCapacity(1000);
        // configuration.getEngineDefaults().getThreading().setThreadPoolInboundNumThreads(Runtime.getRuntime().availableProcessors());

        //configuration.getEngineDefaults().getExecution().setDisableLocking(true);

        String epl = FileUtil.readResourceAsString("epl_case9_follow_by_1.sql");

        int cpuCores = Runtime.getRuntime().availableProcessors();
        CountDownLatch cdt = new CountDownLatch(cpuCores);
        CountDownLatch endCdt = new CountDownLatch(1);
        Map<Integer, EPRuntime> engines = new HashMap<>();

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
                engines.put(engineIndex, epService.getEPRuntime());

                try {
                    for (int j = engineIndex * numTaskPerCore; j < engineIndex * (numTaskPerCore + 1); ++j) {
                        String regularEpl = MessageFormat.format(epl, j, j);
                        final String eplName = "EPL of Engine#" + j;
                        EPStatement epStatement = epService.getEPAdministrator().createEPL(regularEpl, eplName);
                        epStatement.addListener((newData, oldData, stat, rt) -> {
                            MetricUtil.getCounter("Detected patterns " + eplName).inc();
                            //System.out.println("selected row: " + JSONObject.toJSONString(newData[0].getUnderlying()));
                        });
                    }
                    cdt.countDown();
                    endCdt.await();
                } catch (Exception e) {
                    throw new RuntimeException("Error on execute eql", e);
                }
            });
        }

        cdt.await();

        // Send events
        long remainingEvents = Long.MAX_VALUE;
        long cnt = 0;
        String[] eventNames = new String[]{"A", "B"};
        while (--remainingEvents > 0) {
            int randomVal = new Random().nextInt(eventNames.length);
            JSONObject element = new JSONObject();
            element.put("event_id", cnt++);

            int index = (int) cnt % 0xFF;
            element.put("event_name", eventNames[randomVal % eventNames.length]);
            element.put("src_address", "172.16.100." + index);
            element.put("dst_address", "172.16.100." + index);
            element.put("occur_time", System.currentTimeMillis() + randomVal);
            MetricUtil.getConsumeRateMetric().mark();

            int engineIndex = (index + 1) / numTaskPerCore;
            EPRuntime runtime = engines.get(engineIndex);
            runtime.sendEvent(element, "TestEvent");
        }
        endCdt.countDown();
    }
}
