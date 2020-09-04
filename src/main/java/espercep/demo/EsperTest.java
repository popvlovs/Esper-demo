package espercep.demo;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.espertech.esper.client.*;
import espercep.demo.mock.MockDataThread;
import espercep.demo.udf.UserDefinedFunction;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class EsperTest {
    public static void main(String[] args) {
        // Set event representation
        Configuration configuration = new Configuration();

        //configuration.getEngineDefaults().getThreading().setListenerDispatchPreserveOrder(false);
        /*configuration.setPatternMaxSubexpressions(100000L);
        configuration.getEngineDefaults().getViewResources().setShareViews(false);
        configuration.getEngineDefaults().getThreading().setListenerDispatchPreserveOrder(true);

        configuration.getEngineDefaults().getExecution().setFairlock(false);
        configuration.getEngineDefaults().getExecution().setThreadingProfile(ConfigurationEngineDefaults.ThreadingProfile.LARGE);
        configuration.getEngineDefaults().getExecution().setFilterServiceProfile(ConfigurationEngineDefaults.FilterServiceProfile.READWRITE);*/

        configuration.addPlugInSingleRowFunction("GET_BASELINE", UserDefinedFunction.class.getName(), "getBaseline", ConfigurationPlugInSingleRowFunction.ValueCache.ENABLED);

        String recordNums = Arrays.stream(args).filter(arg -> arg.startsWith("recordNums=")).findFirst().orElse("recordNums=15000000");
        int num = Integer.parseInt(recordNums.replaceAll("recordNums=", ""));

        String filterRatio = Arrays.stream(args).filter(arg -> arg.startsWith("filterRatio=")).findFirst().orElse("filterRatio=1");
        int ratio = Integer.parseInt(filterRatio.replaceAll("filterRatio=", ""));

        EPServiceProvider epService = EPServiceProviderManager.getProvider("esper", configuration);
        Map<String, Object> eventType = new HashMap<>();
        eventType.put("event_name", String.class);
        eventType.put("event_id", Long.class);
        eventType.put("src_address", String.class);
        eventType.put("dst_address", String.class);
        eventType.put("occur_time", Long.class);
        epService.getEPAdministrator().getConfiguration().addEventType("TestEvent", eventType);
        // epService.getEPRuntime().setUnmatchedListener(new UnMatchedEventListener());

        // Compile statement
        String statements = args.length > 0 ? args[0] : "[\n" +
                "    {\n" +
                "        \"name\": \"时间窗口内邮件发送次数\",\n" +
                "        \"epl\": \"SELECT count(*) as total, min(occur_time) as start_time, max(occur_time) as end_time, A.src_address as src_address FROM TestEvent(event_name='邮件发送').win:ext_timed(occur_time, 1 min) as A GROUP BY A.src_address HAVING COUNT(*) > 100 AND COUNT(DISTINCT(dst_address)) > 5\",\n" +
                "        \"repeat\": 1\n" +
                "    }\n" +
                "]";
        boolean isPrint = !Arrays.stream(args).anyMatch(arg -> arg.equals("showResult"));

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
                        if (isPrint) {
                            System.out.println("[" + nameWithNum + "] selected row: " + JSONObject.toJSONString(newData[0].getUnderlying()));
                        }
                    });
                    repeats--;
                }
            }
            // Send events
            new MockDataThread(num, ratio, epService.getEPRuntime()).start();
        } catch (Exception e) {
            throw new RuntimeException("Error on execute eql", e);
        }
    }
}
