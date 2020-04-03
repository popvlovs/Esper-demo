package espercep.demo.cases.esper;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.espertech.esper.client.*;
import espercep.demo.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class TimeWindowViewUpdate_1 {
    private static final Logger logger = LoggerFactory.getLogger(TimeWindowViewUpdate_1.class);

    public static void main(String[] args) throws Exception {
        // Set event representation
        Configuration configuration = new Configuration();

        EPServiceProvider epService = EPServiceProviderManager.getProvider("esper", configuration);
        Map<String, Object> eventType = new HashMap<>();
        eventType.put("event_name", String.class);
        eventType.put("event_id", Long.class);
        eventType.put("occur_time", Long.class);
        epService.getEPAdministrator().getConfiguration().addEventType("TestEvent", eventType);

        try {
            String epl1 = FileUtil.readResourceAsString("epl_case6_ext_timed.sql");
            EPStatement epStatement = epService.getEPAdministrator().createEPL(epl1, "EPL#1");
            epStatement.addListener((newData, oldData) -> {
                // Inbound
                Arrays.stream(Optional.ofNullable(newData).orElse(new EventBean[]{})).forEach(
                        data -> logger.info("\n\ninbounds:\n{}\n", JSON.toJSONString(data.getUnderlying()))
                );

                // Outbound
                Arrays.stream(Optional.ofNullable(oldData).orElse(new EventBean[]{})).forEach(
                        data -> logger.info("\n\noutbounds:\n{}\n", JSON.toJSONString(data.getUnderlying()))
                );
            });

            // Send events
            sendEvents(epService.getEPRuntime());
        } catch (
                Exception e) {
            throw new RuntimeException("Error on execute eql", e);
        }
    }

    private static void sendEvents(EPRuntime esperRuntime) throws Exception {
        int eventId = 0;

        // Send event 1
        JSONObject element1 = new JSONObject();
        element1.put("event_id", eventId++);
        element1.put("event_name", "B");
        element1.put("occur_time", System.currentTimeMillis());
        logger.info(element1.toJSONString());
        esperRuntime.sendEvent(element1, "TestEvent");

        // Send event 2
        JSONObject element2 = new JSONObject();
        element2.put("event_id", eventId++);
        element2.put("event_name", "A");
        element2.put("occur_time", System.currentTimeMillis() + 1000);
        logger.info(element2.toJSONString());
        esperRuntime.sendEvent(element2, "TestEvent");

        // Send event 3
        JSONObject element3 = new JSONObject();
        element3.put("event_id", eventId++);
        element3.put("event_name", "A");
        element3.put("occur_time", System.currentTimeMillis() + 2000);
        logger.info(element3.toJSONString());
        esperRuntime.sendEvent(element3, "TestEvent");

        // Send event 4
        JSONObject element4 = new JSONObject();
        element4.put("event_id", eventId++);
        element4.put("event_name", "B");
        element4.put("occur_time", System.currentTimeMillis() + 3000);
        logger.info(element4.toJSONString());
        esperRuntime.sendEvent(element4, "TestEvent");

        // Send event 5
        JSONObject element5 = new JSONObject();
        element5.put("event_id", eventId++);
        element5.put("event_name", "B");
        element5.put("occur_time", System.currentTimeMillis() + 4000);
        logger.info(element5.toJSONString());
        esperRuntime.sendEvent(element5, "TestEvent");
    }
}
