package espercep.demo.cases;

import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.Meter;
import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import espercep.demo.cases.ipc.chronicle.*;
import espercep.demo.state.CountWindowGroupState;
import espercep.demo.util.ArgsUtil;
import espercep.demo.util.CmdLineOptions;
import espercep.demo.util.FileUtil;
import espercep.demo.util.MetricUtil;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 * <p>
 * 使用自定义CountWindowGroupState实现count-window的demo
 *
 * @author yitian_song
 */
public class WindowCountMetric_IPC_Consumer_1 {
    private static final Logger logger = LoggerFactory.getLogger(WindowCountMetric_IPC_Consumer_1.class);
    private static CmdLineOptions options;
    private static ChronicleMessageChannel<Map> channel;

    public static void main(String[] args) throws Exception {
        options = ArgsUtil.getArg(args);
        logger.info("Using args as {}", options);
        channel = ChronicleMessageChannelBuilders.builder(Map.class)
                .topic("A")
                .buildConsumer(options.getChronicleId());

        MetricUtil.disable(options.isNoMetric());
        startConsumer();

        System.exit(0);
    }

    private static void startConsumer() throws Exception {
        // Start esper engine
        Configuration configuration = new Configuration();
        String epl = FileUtil.readResourceAsString("epl_case16_count_window.sql");
        try {
            EPServiceProvider epService = EPServiceProviderManager.getProvider("esper#" + 0, configuration);
            Map<String, Object> eventType = new HashMap<>();
            eventType.put("event_name", String.class);
            eventType.put("event_id", Long.class);
            for (int groupIndex = 0; groupIndex < options.getGroupByNum(); groupIndex++) {
                eventType.put("group_" + groupIndex, Integer.class);
            }
            eventType.put("src_address", String.class);
            eventType.put("dst_address", String.class);
            eventType.put("occur_time", Long.class);
            epService.getEPAdministrator().getConfiguration().addEventType("TestEvent", eventType);

            for (int ruleIdx = 0; ruleIdx < options.getRuleNum(); ruleIdx++) {
                createStatement(epService, epl, 0, ruleIdx);
            }

            // Create chronicle consumer
            Meter meter = MetricUtil.getMeter("Consumer");
            channel.consume(data -> {
                meter.mark();
                epService.getEPRuntime().sendEvent(data, "TestEvent");
            });
        } catch (Exception e) {
            throw new RuntimeException("Error on execute eql", e);
        }
    }

    private static EPStatement createStatement(EPServiceProvider epService, String epl, int engineIdx, int ruleIdx) {
        final String ruleName = "CountWindow#" + ruleIdx;
        CountWindowGroupState<Map> stateOfRule = new CountWindowGroupState<>()
                .groupBy(getGroupKeys())
                .outputLastAs("event_id", "last_event_id")
                .outputLastAs("src_address", "last_src_address")
                .outputLastAs("dst_address", "last_dst_address")
                .outputLastAs("occur_time", "last_occur_time")
                .outputLastAs("event_name", "last_event_name")
                .outputWindowAs("event_id", "event_ids")
                .outputWindowAs("src_address", "src_address_arr")
                .outputWindowAs("dst_address", "dst_address_arr")
                .outputWindowAs("occur_time", "occur_time")
                .outputWindowAs("event_name", "event_name")
                .outputWindowAs("group_0", "group_0")
                .outputWindowAs("group_1", "group_1")
                .outputWindowAs("group_2", "group_2")
                .outputCountAs("count");
        EPStatement epStatement = epService.getEPAdministrator().createEPL(epl, ruleName, stateOfRule);
        epStatement.addListener((newData, oldData, stat, rt) -> {
            CountWindowGroupState<Map> state = (CountWindowGroupState<Map>) stat.getUserObject();
            // Inbound
            Arrays.stream(newData).forEach(data -> {
                MetricUtil.getCounter(String.format("Engine #%d, Rule #%s inbound", engineIdx, ruleIdx)).inc();

                Map dataMap = (Map) data.getUnderlying();
                String eventName = dataMap.get("event_name").toString();
                List<Map<String, Object>> result = state.applyEntry(dataMap);
                if (result != null) {
                    result.forEach(output -> {
                        long count = (long) output.get("count");
                        MetricUtil.getCounter(String.format("Engine#%d, rule#%d, %s output total", engineIdx, ruleIdx, eventName)).inc(count);
                        MetricUtil.getCounter(String.format("Engine#%d, rule#%d, %s output times", engineIdx, ruleIdx, eventName)).inc();
                    });
                }
            });
        });
        return epStatement;
    }

    private static String[] getGroupKeys() {
        String[] keys = new String[options.getGroupByNum()];
        for (int i = 0; i < options.getGroupByNum(); i++) {
            keys[i] = "group_" + i;
        }
        return keys;
    }
}
