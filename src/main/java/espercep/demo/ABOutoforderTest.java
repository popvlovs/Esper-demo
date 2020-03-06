package espercep.demo;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.espertech.esper.client.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class ABOutoforderTest {
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
                "        \"name\": \"EDR Case: A->B\",\n" +
                //"        \"epl\": \"select A.occur_time as start_time, D.occur_time as end_time, A.event_id as event_id_A, B.event_id as event_id_B, C.event_id as event_id_C, D.event_id as event_id_D from pattern[every(A=TestEvent(event_name='A') and B=TestEvent(event_name='B') and C=TestEvent(event_name='C') and D=TestEvent(event_name='D') where timer:within(1 sec)) while(A.occur_time <= B.occur_time and B.occur_time <= C.occur_time and C.occur_time <= D.occur_time and D.occur_time - A.occur_time <= 2000)]\",\n" +
                //"        \"epl\": \"select A.occur_time as start_time, D.occur_time as end_time, A.event_id as event_id_A, B.event_id as event_id_B, C.event_id as event_id_C, D.event_id as event_id_D from pattern[every(A=TestEvent(event_name='A') and B=TestEvent(event_name='B') and C=TestEvent(event_name='C') and D=TestEvent(event_name='D') where timer:within(1 sec)) while(A.occur_time <= B.occur_time and B.occur_time <= C.occur_time and C.occur_time <= D.occur_time and D.occur_time - A.occur_time <= 2000 and A.src_address = B.dst_address)]\",\n" +
                //"        \"epl\": \"select A.occur_time as start_time, B.occur_time as end_time, A.event_id as event_id_A, B.event_id as event_id_B from pattern[every ((every A=TestEvent(event_name='A') and every B=TestEvent(event_name='B')) where timer:within(1 sec)) while (A.occur_time <= B.occur_time)]\",\n" +
                "        \"epl\": \"select A.occur_time as start_time, B.occur_time as end_time, A.event_id as event_id_A, B.event_id as event_id_B from pattern[every ((every A=TestEvent(event_name='A') and every B=TestEvent(event_name='B')) where timer:within(1 sec)) while (A.occur_time <= B.occur_time and A.src_address = B.dst_address)]\",\n" +
                //"        \"epl\": \"select A.occur_time as start_time, D.occur_time as end_time, A.event_id as event_id_A, B.event_id as event_id_B, C.event_id as event_id_C, D.event_id as event_id_D from pattern[every A=TestEvent(event_name='A') and every B=TestEvent(event_name='B') and every C=TestEvent(event_name='C') and every D=TestEvent(event_name='D')]\",\n" +
                //"        \"epl\": \"select * from TestEvent match_recognize (measures A.event_id as a_id, B.event_id as b_id, C.event_id as c_id, D.event_id as d_id pattern (match_recognize_permute(A,B,C,D)) interval 5 seconds or terminated define A as A.event_name = 'A', B as B.event_name = 'B' and B.occur_time >= A.occur_time, C as C.event_name = 'C' and C.occur_time >= B.occur_time, D as D.event_name = 'D' and D.occur_time >= C.occur_time)\",\n" +
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

    private static void sendEvents(EPRuntime esperRuntime) throws InterruptedException {
        Long basetime = System.currentTimeMillis();

        // Send event A
        JSONObject elementA0 = new JSONObject();
        elementA0.put("event_id", 1);
        elementA0.put("event_name", "A");
        elementA0.put("src_address", "172.16.101.1");
        elementA0.put("dst_address", "172.16.101.1");
        elementA0.put("occur_time", basetime + 100L);
        esperRuntime.sendEvent(elementA0, "TestEvent");

        // Send event B
        JSONObject elementB2 = new JSONObject();
        elementB2.put("event_id", 2);
        elementB2.put("event_name", "B");
        elementB2.put("src_address", "172.16.100.0");
        elementB2.put("dst_address", "172.16.101.1");
        elementB2.put("occur_time", basetime + 200L);
        esperRuntime.sendEvent(elementB2, "TestEvent");

        // Send event A
        JSONObject elementA = new JSONObject();
        elementA.put("event_id", 3);
        elementA.put("event_name", "A");
        elementA.put("src_address", "172.16.101.1");
        elementA.put("dst_address", "172.16.101.1");
        elementA.put("occur_time", basetime + 100L);
        esperRuntime.sendEvent(elementA, "TestEvent");
    }
}
