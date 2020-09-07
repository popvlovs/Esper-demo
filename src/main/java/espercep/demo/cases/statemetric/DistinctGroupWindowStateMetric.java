package espercep.demo.cases.statemetric;

import com.alibaba.fastjson.JSONObject;
import com.espertech.esper.client.*;
import com.espertech.esper.metrics.statement.StatementStateMetric;
import com.espertech.esper.util.FeatureToggle;
import espercep.demo.util.FileUtil;
import espercep.demo.util.MetricUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 * <p>
 * 使用自定义CountWindowGroupState实现count-window的demo
 *
 * @author yitian_song
 */
public class DistinctGroupWindowStateMetric {
    private static final Logger logger = LoggerFactory.getLogger(DistinctGroupWindowStateMetric.class);

    public static void main(String[] args) throws Exception {
        // Set event representation
        Configuration configuration = new Configuration();

        FeatureToggle.setNumDistinctEventRetained(10);
        FeatureToggle.setDiscardExtTimedWindowOnAggOutput(true);

        EPServiceProvider epService = EPServiceProviderManager.getProvider("esper-engine", configuration);
        Map<String, Object> eventType = new HashMap<>();
        eventType.put("event_name", String.class);
        eventType.put("event_id", Long.class);
        eventType.put("src_address", String.class);
        eventType.put("dst_address", String.class);
        eventType.put("occur_time", Long.class);
        epService.getEPAdministrator().getConfiguration().addEventType("TestEvent", eventType);

        String epl = FileUtil.readResourceAsString("epl_case18_window_count.sql");
        System.out.println(epl);

        EPStatement epStatement = epService.getEPAdministrator().createEPL(epl, "Rule#1");
        epStatement.addListener((newData, oldData, stat, rt) -> {
            MetricUtil.getCounter("Detected patterns Rule#1").inc();
            //System.out.println("selected row: " + JSONObject.toJSONString(newData[0].getUnderlying()));
        });

        Executors.newScheduledThreadPool(1).scheduleWithFixedDelay(() -> {
            System.out.println(JSONObject.toJSONString(StatementStateMetric.getAllMetrics(), true));
        }, 5, 5, TimeUnit.SECONDS);

        sendRandomEvents(epService.getEPRuntime());
    }

    private static Random rand = new Random();

    private static void sendRandomEvents(EPRuntime epRuntime) {
        long remainingEvents = Long.MAX_VALUE;
        long cnt = 0;
        long now = System.currentTimeMillis();
        String[] eventNames = new String[]{"A", "B"};
        while (--remainingEvents > 0) {
            int randomVal = rand.nextInt(eventNames.length);
            JSONObject element = new JSONObject();
            element.put("event_id", cnt++);
            element.put("event_name", eventNames[randomVal % eventNames.length]);
            element.put("src_address", "172.16.100." + cnt % 0xFF);
            element.put("src_address", null);
            element.put("dst_address", "172.16.100." + cnt % 0xFF);
            //element.put("occur_time", System.currentTimeMillis() + rand.nextInt(60000) - TimeUnit.SECONDS.toMillis(1));// + randomVal);
            //element.put("occur_time", System.currentTimeMillis());
            element.put("occur_time", now ++);
            MetricUtil.getConsumeRateMetric().mark();
            epRuntime.sendEvent(element, "TestEvent");
        }
    }
}
