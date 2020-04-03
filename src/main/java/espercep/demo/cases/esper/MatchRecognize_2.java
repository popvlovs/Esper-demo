package espercep.demo.cases.esper;

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
public class MatchRecognize_2 {
    public static void main(String[] args) {
        // Set event representation
        Configuration configuration = new Configuration();

        // Multi-thread may cause detection missing
        //configuration.getEngineDefaults().getThreading().setThreadPoolInbound(true);
        //configuration.getEngineDefaults().getThreading().setThreadPoolInboundCapacity(1000);
        //configuration.getEngineDefaults().getThreading().setThreadPoolInboundNumThreads(Runtime.getRuntime().availableProcessors());

        EPServiceProvider epService = EPServiceProviderManager.getProvider("esper", configuration);
        Map<String, Object> eventType = new HashMap<>();
        eventType.put("id", String.class);
        eventType.put("device", Long.class);
        eventType.put("temp", Long.class);
        epService.getEPAdministrator().getConfiguration().addEventType("TestEvent", eventType);

        try {
            String epl = FileUtil.readResourceAsString("eql_case3_permute_example.sql");
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
        // Send event 1
        JSONObject element1 = new JSONObject();
        element1.put("id", "E1");
        element1.put("device", 1L);
        element1.put("temp", 99L);
        esperRuntime.sendEvent(element1, "TestEvent");

        // Send event 2
        JSONObject element2 = new JSONObject();
        element2.put("id", "E2");
        element2.put("device", 1L);
        element2.put("temp", 100L);
        esperRuntime.sendEvent(element2, "TestEvent");

        // Send event 3
        JSONObject element3 = new JSONObject();
        element3.put("id", "E3");
        element3.put("device", 1L);
        element3.put("temp", 100L);
        esperRuntime.sendEvent(element3, "TestEvent");

        // Send event 4
        JSONObject element4 = new JSONObject();
        element4.put("id", "E4");
        element4.put("device", 1L);
        element4.put("temp", 99L);
        esperRuntime.sendEvent(element4, "TestEvent");

        // Send event 5
        JSONObject element5 = new JSONObject();
        element5.put("id", "E5");
        element5.put("device", 1L);
        element5.put("temp", 98L);
        esperRuntime.sendEvent(element5, "TestEvent");
    }
}
