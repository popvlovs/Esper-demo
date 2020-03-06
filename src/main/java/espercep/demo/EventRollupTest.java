package espercep.demo;

import com.alibaba.fastjson.JSONObject;
import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import espercep.demo.mock.MockDataThread;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class EventRollupTest {
    public static void main(String[] args) {
        // Set event representation
        Configuration configuration = new Configuration();

        System.out.println("Enable multiple thread:");
        configuration.getEngineDefaults().getThreading().setThreadPoolInbound(true);
        configuration.getEngineDefaults().getThreading().setThreadPoolInboundCapacity(1000);
        configuration.getEngineDefaults().getThreading().setThreadPoolInboundNumThreads(Runtime.getRuntime().availableProcessors());

        int num = 10_000_000;
        int ratio = 1;

        EPServiceProvider epService = EPServiceProviderManager.getProvider("esper", configuration);

        // Full event
        Map<String, Object> eventType = new HashMap<>();
        eventType.put("event_name", String.class);
        eventType.put("event_id", Long.class);
        eventType.put("src_address", String.class);
        eventType.put("dst_address", String.class);
        eventType.put("occur_time", Long.class);
        epService.getEPAdministrator().getConfiguration().addEventType("TestEvent", eventType);

        // Reduced event
        Map<String, Object> reducedEventType = new HashMap<>();
        reducedEventType.put("event_name", String.class);
        reducedEventType.put("src_address", String.class);
        reducedEventType.put("dst_address", String.class);
        reducedEventType.put("start_time", Long.class);
        reducedEventType.put("end_time", Long.class);
        reducedEventType.put("event_cnt", Integer.class);
        epService.getEPAdministrator().getConfiguration().addEventType("ReducedEvent", reducedEventType);


        // Compile statement
        String statements = args.length > 0 ? args[0] : "[\n" +
                "    {\n" +
                "        \"name\": \"不加NamedWindow执行100次\",\n" +
                "        \"epl\": \"SELECT count(*) as total, min(occur_time) as start_time, max(occur_time) as end_time, A.src_address as src_address FROM TestEvent(event_name='邮件发送').win:ext_timed(occur_time, 1 min) as A GROUP BY A.src_address\",\n" +
                "        \"repeat\": 100\n" +
                "    }\n" +
                "]";
        boolean isPrint = Arrays.stream(args).anyMatch(arg -> arg.equals("showResult"));

        try {
            String reduceSql = "@name('ReducedEvent') select count(*) as event_cnt, min(occur_time) as start_time, max(occur_time) as end_time, src_address, dst_address, event_name from TestEvent.win:ext_timed(occur_time, 1 sec) group by event_name, src_address, dst_address having count(*) > 50";
            EPStatement epStatement = epService.getEPAdministrator().createEPL(reduceSql);
            epStatement.addListener((newData, oldData, stat, rt) -> {
                System.out.println("selected row: " + JSONObject.toJSONString(newData[0].getUnderlying()));
            });

            /*JSONArray statementJA = JSONArray.parseArray(statements);
            for (int i = 0; i < statementJA.size(); i++) {
                JSONObject statementJO = statementJA.getJSONObject(i);
                String epl = statementJO.getString("epl");
                String name = statementJO.getString("name");
                int repeats = statementJO.getIntValue("repeat");
                repeats = repeats <= 0 ? 1 : repeats;
                while (repeats > 0) {
                    String nameWithNum = name + "#" + repeats;
                    EPStatement epStatement = epService.getEPAdministrator().createEPL(epl, nameWithNum);
                    epStatement.addListener((newData, oldData, stat, rt) -> {
                        if (isPrint) {
                            System.out.println("[" + nameWithNum + "] selected row: " + JSONObject.toJSONString(newData[0].getUnderlying()));
                        }
                    });
                    repeats--;
                }
            }*/
            // Send events
            new MockDataThread(num, ratio, epService.getEPRuntime()).start();
        } catch (Exception e) {
            throw new RuntimeException("Error on execute eql", e);
        }
    }
}
