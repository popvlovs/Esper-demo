package espercep.demo;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.espertech.esper.client.*;
import espercep.demo.reorder.OrderedQueue;

import java.util.HashMap;
import java.util.Map;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class ReorderTest {
    public static void main(String[] args) {
        // Set event representation
        Configuration configuration = new Configuration();

        EPServiceProvider epService = EPServiceProviderManager.getProvider("esper", configuration);
        Map<String, Object> eventType = new HashMap<>();
        eventType.put("event_name", String.class);
        eventType.put("event_id", Long.class);
        eventType.put("src_address", String.class);
        eventType.put("dst_address", String.class);
        eventType.put("occur_time", Long.class);
        epService.getEPAdministrator().getConfiguration().addEventType("TestEvent", eventType);

        // Compile statement
        String statements = args.length > 0 ? args[0] : "[\n" +
                "    {\n" +
                "        \"name\": \"邮件登陆->邮件发送\",\n" +
                "        \"epl\": \"select A.occur_time as start_time, B.occur_time as end_time, A.event_id as event_id_A, B.event_id as event_id_B from pattern[every A=TestEvent(event_name='邮件登陆')->(B=TestEvent(event_name='邮件发送') where timer:within(5 sec)) while (A.occur_time<=B.occur_time and B.occur_time-A.occur_time <= 1000)]\",\n" +
                "        \"repeat\": 1\n" +
                "    }\n" +
                "]";

        try {
            JSONArray statementJA = JSONArray.parseArray(statements);
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
                        System.out.println("[" + nameWithNum + "] selected row: " + JSONObject.toJSONString(newData[0].getUnderlying()));
                    });
                    repeats--;
                }
            }
            // Send events
            sendEvents(epService.getEPRuntime());
        } catch (Exception e) {
            throw new RuntimeException("Error on execute eql", e);
        }
    }

    private static void sendEvents(EPRuntime esperRuntime) {
        Long basetime = System.currentTimeMillis();

        // Send event B
        JSONObject elementB = new JSONObject();
        elementB.put("event_id", 1);
        elementB.put("event_name", "邮件发送");
        elementB.put("src_address", "172.16.100.0");
        elementB.put("dst_address", "172.16.101.1");
        elementB.put("occur_time", basetime + 500L);
        sendEvent(esperRuntime, elementB);

        // Send event A
        JSONObject elementA = new JSONObject();
        elementA.put("event_id", 0);
        elementA.put("event_name", "邮件登陆");
        elementA.put("src_address", "172.16.100.0");
        elementA.put("dst_address", "172.16.101.1");
        elementA.put("occur_time", basetime + 100L);
        sendEvent(esperRuntime, elementA);
    }

    private static OrderedQueue queue;

    private static void sendEvent(EPRuntime esperRuntime, JSONObject element) {
        // with order
        if (queue == null) {
            queue = OrderedQueue.onEventDeque(event -> esperRuntime.sendEvent(event, "TestEvent"));
        }
        queue.bufferEvent(element, element.getLongValue("occur_time"));
    }
}
