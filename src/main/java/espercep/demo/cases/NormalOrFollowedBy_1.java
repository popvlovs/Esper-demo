package espercep.demo.cases;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.espertech.esper.client.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 *  尽量将关联条件前置，ABC 1:1:1随机生成
 *
 * @author yitian_song
 */
public class NormalOrFollowedBy_1 {
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
                "        \"name\": \"EDR Case: A->B or B->A\",\n" +
                "        \"epl\": \"" +
                "    select A.occur_time as start_time, B.occur_time as end_time, A.event_id as event_id_A, B.event_id as event_id_B from pattern[\n" +
                "    \n" +
                "        (every A=TestEvent(event_name='A') -> B=TestEvent(event_name='B' and A.src_address = B.dst_address) where timer:within(5 sec))\n" +
                "        or\n" +
                "        (every B=TestEvent(event_name='B') -> A=TestEvent(event_name='A' and A.src_address = B.dst_address) where timer:within(5 sec))\n" +
                "    ]\",\n" +
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
        String[] eventNames = new String[]{"A", "B"};
        while (--remainingEvents > 0) {
            int randomVal = new Random().nextInt(2);
            JSONObject element = new JSONObject();
            element.put("event_id", cnt++);
            element.put("event_name", eventNames[randomVal%2]);
            element.put("src_address", "172.16.100." + cnt%0xFF);
            element.put("dst_address", "172.16.100." + cnt%0xFF);
            element.put("occur_time", System.currentTimeMillis() + randomVal);
            consumeRate.mark();
            epRuntime.sendEvent(element, "TestEvent");
        }
    }

    private static void sendEvents(EPRuntime esperRuntime) throws InterruptedException {
        Long basetime = System.currentTimeMillis();
        int eventId = 0;

        // Send event A
        JSONObject elementA = new JSONObject();
        elementA.put("event_id", eventId++);
        elementA.put("event_name", "A");
        elementA.put("src_address", "172.16.101.1");
        elementA.put("dst_address", "172.16.101.1");
        elementA.put("occur_time", basetime + 100L);
        esperRuntime.sendEvent(elementA, "TestEvent");

        // Send event B
        JSONObject elementB2 = new JSONObject();
        elementB2.put("event_id", eventId++);
        elementB2.put("event_name", "B");
        elementB2.put("src_address", "172.16.100.0");
        elementB2.put("dst_address", "172.16.101.1");
        elementB2.put("occur_time", basetime + 200L);
        esperRuntime.sendEvent(elementB2, "TestEvent");
    }
}
