package espercep.demo.cases.esper;

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
 * <p>
 * 尽量将关联条件前置，ABC 1:1:1随机生成
 *
 * @author yitian_song
 */
public class OutOfOrderFollowedBy_2 {

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
                "        \"name\": \"EDR Case: A->B->C\",\n" +
                "        \"epl\": \"" +
                "    select A.occur_time as start_time, C.occur_time as end_time, A.event_id as event_id_A, B.event_id as event_id_B, C.event_id as event_id_C from pattern[\n" +
                "    (\n" +
                "        (every A=TestEvent(event_name='A') -> B=TestEvent(event_name='B') -> C=TestEvent(event_name='C' and A.src_address = B.dst_address and B.src_address = C.dst_address) where timer:within(5 sec))\n" +
                "        or\n" +
                "        (every A=TestEvent(event_name='A') -> C=TestEvent(event_name='C') -> B=TestEvent(event_name='B' and A.src_address = B.dst_address and B.src_address = C.dst_address) where timer:within(5 sec))\n" +
                "        or\n" +
                "        (every B=TestEvent(event_name='B') -> A=TestEvent(event_name='A') -> C=TestEvent(event_name='C' and A.src_address = B.dst_address and B.src_address = C.dst_address) where timer:within(5 sec))\n" +
                "        or\n" +
                "        (every B=TestEvent(event_name='B') -> C=TestEvent(event_name='C') -> A=TestEvent(event_name='A' and A.src_address = B.dst_address and B.src_address = C.dst_address) where timer:within(5 sec))\n" +
                "        or\n" +
                "        (every C=TestEvent(event_name='C') -> A=TestEvent(event_name='A') -> B=TestEvent(event_name='B' and A.src_address = B.dst_address and B.src_address = C.dst_address) where timer:within(5 sec))\n" +
                "        or\n" +
                "        (every C=TestEvent(event_name='C') -> B=TestEvent(event_name='B') -> A=TestEvent(event_name='A' and A.src_address = B.dst_address and B.src_address = C.dst_address) where timer:within(5 sec))\n" +
                "    ) while (A.occur_time <= B.occur_time and B.occur_time <= C.occur_time)]" +
                "        \",\n" +
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
        String[] eventNames = new String[]{"A", "B", "C"};
        while (--remainingEvents > 0) {
            int randomVal = new Random().nextInt(3);
            JSONObject element = new JSONObject();
            element.put("event_id", cnt++);
            element.put("event_name", eventNames[randomVal % 3]);
            element.put("src_address", "172.16.100." + cnt % 0xFF);
            element.put("dst_address", "172.16.100." + cnt % 0xFF);
            element.put("occur_time", System.currentTimeMillis() + randomVal);
            consumeRate.mark();
            epRuntime.sendEvent(element, "TestEvent");
        }
    }

    private static void sendEvents(EPRuntime esperRuntime) throws InterruptedException {
        Long basetime = System.currentTimeMillis();
        int eventId = 0;

        // Send event A
        JSONObject elementA0 = new JSONObject();
        elementA0.put("event_id", eventId++);
        elementA0.put("event_name", "A");
        elementA0.put("src_address", "172.16.100.0");
        elementA0.put("dst_address", "172.16.101.1");
        elementA0.put("occur_time", basetime + 100L);
        esperRuntime.sendEvent(elementA0, "TestEvent");

        // Send event A
        JSONObject elementA = new JSONObject();
        elementA.put("event_id", eventId++);
        elementA.put("event_name", "A");
        elementA.put("src_address", "172.16.101.1");
        elementA.put("dst_address", "172.16.101.1");
        elementA.put("occur_time", basetime + 150L);
        esperRuntime.sendEvent(elementA, "TestEvent");

        // Send event B
        JSONObject elementB3 = new JSONObject();
        elementB3.put("event_id", eventId++);
        elementB3.put("event_name", "B");
        elementB3.put("src_address", "172.16.100.5");
        elementB3.put("dst_address", "172.16.101.5");
        elementB3.put("occur_time", basetime + 200L);
        esperRuntime.sendEvent(elementB3, "TestEvent");

        // Send event B
        JSONObject elementB2 = new JSONObject();
        elementB2.put("event_id", eventId++);
        elementB2.put("event_name", "B");
        elementB2.put("src_address", "172.16.100.0");
        elementB2.put("dst_address", "172.16.101.1");
        elementB2.put("occur_time", basetime + 200L);
        esperRuntime.sendEvent(elementB2, "TestEvent");

        // Send event C
        JSONObject elementC1 = new JSONObject();
        elementC1.put("event_id", eventId++);
        elementC1.put("event_name", "C");
        elementC1.put("src_address", "172.16.101.1");
        elementC1.put("dst_address", "172.16.100.0");
        elementC1.put("occur_time", basetime + 300L);
        esperRuntime.sendEvent(elementC1, "TestEvent");
    }
}
