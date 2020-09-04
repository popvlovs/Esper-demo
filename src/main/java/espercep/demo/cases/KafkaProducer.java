package espercep.demo.cases;

import com.espertech.esper.client.*;
import com.espertech.esper.util.FeatureToggle;
import espercep.demo.util.FileUtil;
import espercep.demo.util.MetricUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class KafkaProducer {
    public static void main(String[] args) throws Exception {
        // Set event representation
        Configuration configuration = new Configuration();

        FeatureToggle.setDiscardExtTimedWindowOnAggOutput(true);
        FeatureToggle.setNumDistinctEventRetained(10);
        EPServiceProvider epService = EPServiceProviderManager.getProvider("esper-engine", configuration);

        configuration.getEngineDefaults().getMetricsReporting().setEnableMetricsReporting(true);
        configuration.getEngineDefaults().getMetricsReporting().setEngineInterval(1000);
        configuration.getEngineDefaults().getMetricsReporting().setStatementInterval(1000);

        // Schema
        Map<String, Object> eventType = new HashMap<>();
        eventType.put("nta", String.class);
        eventType.put("app_protocol", String.class);
        eventType.put("id", String.class);
        eventType.put("collector_source", String.class);
        eventType.put("domain", String.class);
        eventType.put("src_address", String.class);
        eventType.put("dst_address", String.class);
        eventType.put("event_name", String.class);
        eventType.put("sensor_id", String.class);
        eventType.put("host_name", String.class);
        eventType.put("domain_name", String.class);
        eventType.put("rule_id", String.class);
        eventType.put("event_digest", String.class);
        eventType.put("src_mac", String.class);
        eventType.put("event_type", String.class);
        eventType.put("rule_name", String.class);
        eventType.put("unit", String.class);
        eventType.put("dev_address", String.class);
        eventType.put("vendor", String.class);
        eventType.put("dns_lookup_type", String.class);
        eventType.put("data_source", String.class);
        eventType.put("dst_mac", String.class);
        eventType.put("protocol", String.class);
        eventType.put("event_id", String.class);
        eventType.put("src_address_array", String.class);
        eventType.put("dst_address_array", String.class);
        eventType.put("data_source_array", String.class);

        eventType.put("occur_time", Long.class);
        eventType.put("receive_time", Long.class);
        eventType.put("end_time", Long.class);
        eventType.put("first_time", Long.class);
        eventType.put("flow_id", Long.class);
        eventType.put("spin_tag", Long.class);

        eventType.put("event_level", Integer.class);
        eventType.put("src_port", Integer.class);
        eventType.put("dst_port", Integer.class);
        eventType.put("alarm_level", Integer.class);

        epService.getEPAdministrator().getConfiguration().addEventType("GlobalEvent", eventType);

        try {
            String epl = FileUtil.readResourceAsString("epl_case17_172.16.100.216.sql");
            System.out.println(epl);
            for (int i = 0; i < 1; ++i) {
                String regularEpl = epl;
                final String eplName = "EPL#" + i;
                EPStatement epStatement = epService.getEPAdministrator().createEPL(regularEpl, eplName);
                epStatement.addListener((newData, oldData, stat, rt) -> {
                    MetricUtil.getCounter("Detected patterns " + eplName).inc();
                    //System.out.println("selected row: " + JSONObject.toJSONString(newData[0].getUnderlying()));
                });
            }
            sendRandomEvents(epService.getEPRuntime());
        } catch (
                Exception e) {
            throw new RuntimeException("Error on execute eql", e);
        }
    }

    private static Map<String, Object> getImmutablePart() {

        Map<String, Object> element = new HashMap<>();
        element.put("domain", "baidu.com");
        element.put("sensor_id", "54a20667-0e56-4884-8f09-03a46c3586c7");
        element.put("event_level", 0);
        element.put("event_type", "/14YRL6KY0003/8A07ND200015");
        element.put("vendor", "NTA（HanSight）");
        element.put("id", "10000000000000001");
        element.put("unit", "bytes");
        element.put("domain_name", "sp0.baidu.com");
        element.put("protocol", "dns");
        element.put("flow_id", 1586934039473903L);
        element.put("src_mac", "58:FB:84:52:66:65");
        element.put("dst_port", 53);
        element.put("dst_mac", "D4:61:FE:DC:E2:96");
        element.put("collector_source", "test_data");
        element.put("event_name", "DNS查询");
        element.put("rule_name", "nta_dispatcher");
        element.put("nta", "1");
        element.put("data_source", "NTA（HanSight）");
        element.put("dns_lookup_type", "A");
        element.put("event_digest", "nta_dns");
        element.put("app_protocol", "dns");
        element.put("host_name", "sp0");
        element.put("rule_id", "72190fa7-2a8d-457d-8d83-4985ac8c9b48");
        return element;
    }

    private static void sendRandomEvents(EPRuntime epRuntime) {
        long remainingEvents = Long.MAX_VALUE;
        while (--remainingEvents > 0) {
            long now = System.currentTimeMillis();

            int rand = new Random().nextInt(255);
            Map<String, Object> element = getImmutablePart();
            element.put("receive_time", now);
            element.put("occur_time", now - 1000L);
            element.put("dst_address", "61.139.100." + rand);
            element.put("src_port", new Random().nextInt(255) + 30000);
            element.put("end_time", now + 1000L);
            element.put("first_time", now);
            element.put("spin_tag", 0L);
            element.put("alarm_level", 1);
            element.put("src_address", "172.16.0." + new Random().nextInt(255));

            MetricUtil.getConsumeRateMetric().mark();

            epRuntime.sendEvent(element, "GlobalEvent");
        }
    }

}
