package espercep.demo.cases;

import com.alibaba.fastjson.JSONObject;
import com.espertech.esper.client.*;
import espercep.demo.util.FileUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/3/6
 */
public class MatchRecognize_1 {
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
            EPStatement epStatement = epService.getEPAdministrator().createEPL(epl, "EPL#1");
            epStatement.addListener((newData, oldData, stat, rt) -> {
                System.out.println("selected row: " + JSONObject.toJSONString(newData[0].getUnderlying()));
            });
            // Send events
            sendEvents(epService.getEPRuntime());
        } catch (
                Exception e) {
            throw new RuntimeException("Error on execute eql", e);
        }
    }

    private static void sendEvents(EPRuntime esperRuntime) {
        long baseTime = System.currentTimeMillis();
        int eventId = 0;

        // Send event A
        JSONObject elementA0 = new JSONObject();
        elementA0.put("event_id", eventId++);
        elementA0.put("event_name", "A1");
        elementA0.put("src_address", "127.0.0.2");
        elementA0.put("dst_address", "127.0.0.1");
        elementA0.put("occur_time", baseTime + 100L);
        esperRuntime.sendEvent(elementA0, "TestEvent");

        // Send event B
        JSONObject elementB2 = new JSONObject();
        elementB2.put("event_id", eventId++);
        elementB2.put("event_name", "B1");
        elementB2.put("src_address", "127.0.0.2");
        elementB2.put("dst_address", "172.16.101.1");
        elementB2.put("occur_time", baseTime + 200L);
        esperRuntime.sendEvent(elementB2, "TestEvent");
    }
}
