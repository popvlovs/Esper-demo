package espercep.demo.cases;

import com.alibaba.fastjson.JSONObject;
import com.espertech.esper.client.*;
import espercep.demo.util.FileUtil;
import espercep.demo.util.MetricUtil;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class FollowByMetric_Multithread_1 {
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
            String epl = FileUtil.readResourceAsString("epl_case7_follow_by_100.sql");
            for (int i = 0; i < 0xFF; ++i) {
                String regularEpl = MessageFormat.format(epl, i, i);
                final String eplName = "EPL#" + i;
                EPStatement epStatement = epService.getEPAdministrator().createEPL(regularEpl, eplName);
                epStatement.addListener((newData, oldData, stat, rt) -> {
                    MetricUtil.getCounter("Detected patterns " + eplName).inc();
                    //System.out.println("selected row: " + JSONObject.toJSONString(newData[0].getUnderlying()));
                });
            }
            sendRandomEvents(epService.getEPRuntime());
        } catch (
                Exception e) {
            throw new RuntimeException("Error on execute eql", e);
        }
    }

    private static void sendRandomEvents(EPRuntime epRuntime) {
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
            epRuntime.sendEvent(element, "TestEvent");
        }
    }

}
