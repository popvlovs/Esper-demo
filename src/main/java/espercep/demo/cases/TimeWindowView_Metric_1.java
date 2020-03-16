package espercep.demo.cases;

import com.alibaba.fastjson.JSONObject;
import com.espertech.esper.client.*;
import espercep.demo.udf.UserDefinedFunction;
import espercep.demo.util.FileUtil;
import espercep.demo.util.MetricUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class TimeWindowView_Metric_1 {
    private static final Logger logger = LoggerFactory.getLogger(TimeWindowView_Metric_1.class);

    public static void main(String[] args) throws Exception {
        // Set event representation
        Configuration configuration = new Configuration();

        // Define UDF
        configuration.addPlugInSingleRowFunction("oncase", UserDefinedFunction.class.getName(), "onCase", ConfigurationPlugInSingleRowFunction.ValueCache.ENABLED);

        EPServiceProvider epService = EPServiceProviderManager.getProvider("esper", configuration);
        Map<String, Object> eventType = new HashMap<>();
        eventType.put("event_name", String.class);
        eventType.put("src_address", String.class);
        eventType.put("dst_address", String.class);
        eventType.put("event_id", Long.class);
        eventType.put("group", Long.class);
        eventType.put("occur_time", Long.class);
        epService.getEPAdministrator().getConfiguration().addEventType("TestEvent", eventType);

        try {
            String epl1 = FileUtil.readResourceAsString("epl_case6_ext_timed.sql");
            EPStatement epStatement = epService.getEPAdministrator().createEPL(epl1, "EPL#1");
            epStatement.addListener((newData, oldData) -> {
                // Inbound
                Arrays.stream(Optional.ofNullable(newData).orElse(new EventBean[]{})).forEach(
                        data -> {
                            MetricUtil.getCounter("Inbound").inc();
                        }
                );

                // Outbound
                Arrays.stream(Optional.ofNullable(oldData).orElse(new EventBean[]{})).forEach(
                        data -> {
                            MetricUtil.getCounter("Outbound").inc();
                            UserDefinedFunction.onEventExpired((Map) data.getUnderlying());
                        }
                );
            });

            // Send events
            sendRandomEvents(epService.getEPRuntime());
        } catch (Exception e) {
            throw new RuntimeException("Error on execute eql", e);
        }
    }

    private static void sendRandomEvents(EPRuntime runtime) {
        long remainingEvents = Long.MAX_VALUE;
        long cnt = 0;
        String[] eventNames = new String[]{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"};
        while (--remainingEvents > 0) {
            int randomVal = new Random().nextInt(eventNames.length);
            JSONObject element = new JSONObject();
            element.put("event_id", cnt++);
            element.put("group", new Random().nextInt(5));
            element.put("event_name", eventNames[randomVal % eventNames.length]);
            element.put("src_address", "172.16.100." + cnt % 0xFF);
            element.put("dst_address", "172.16.100." + cnt % 0xFF);
            element.put("occur_time", System.currentTimeMillis());
            MetricUtil.getConsumeRateMetric().mark();
            runtime.sendEvent(element, "TestEvent");
        }
    }
}
