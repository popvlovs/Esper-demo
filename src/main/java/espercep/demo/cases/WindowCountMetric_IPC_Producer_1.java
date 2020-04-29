package espercep.demo.cases;

import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.Meter;
import espercep.demo.cases.ipc.chronicle.ChronicleMessageChannel;
import espercep.demo.cases.ipc.chronicle.ChronicleMessageChannelBuilders;
import espercep.demo.cases.ipc.chronicle.CoordinatorStore;
import espercep.demo.util.ArgsUtil;
import espercep.demo.util.CmdLineOptions;
import espercep.demo.util.MetricUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 * <p>
 * 使用自定义CountWindowGroupState实现count-window的demo
 *
 * @author yitian_song
 */
public class WindowCountMetric_IPC_Producer_1 {
    private static final Logger logger = LoggerFactory.getLogger(WindowCountMetric_IPC_Producer_1.class);
    private static CmdLineOptions options;
    private static ChronicleMessageChannel<Map> channel;

    public static void main(String[] args) {
        options = ArgsUtil.getArg(args);
        channel = ChronicleMessageChannelBuilders.builder(Map.class)
                .topic("A")
                .buildProducer(options.getChronicleId());

        logger.info("Using args as {}", options);
        MetricUtil.disable(options.isNoMetric());
        logger.info("Available consumers for topic {}: {}", channel.getTopic(), channel.availableConsumers());
        startProducer();
    }

    private static AtomicLong cnt = new AtomicLong(0L);

    private static void startProducer() {
        Meter meter = MetricUtil.getMeter("Producer");

        channel.produce(() -> {
            if (cnt.get() < options.getEventNum()) {
                meter.mark();
                return mock();
            } else {
                return null;
            }
        });
    }

    private static Map<String, Object> mock() {
        long localCnt = cnt.get();
        final String[] eventNames = new String[]{"A", "B", "C"};
        String eventName = eventNames[(int) localCnt % eventNames.length];
        JSONObject element = new JSONObject();

        element.put("event_id", localCnt++);
        for (int i = 0; i < 3; i++) {
            element.put("group_" + i, localCnt % 16);
        }
        cnt.set(localCnt);

        element.put("event_name", eventName);
        element.put("src_address", "172.16.100." + localCnt % 0xFF);
        element.put("dst_address", "172.16.100." + localCnt % 0xFF);
        //element.put("occur_time", Long.MAX_VALUE - remainingEvents + now);
        element.put("occur_time", System.currentTimeMillis());
        MetricUtil.getCounter(eventName + " inputs").inc();
        MetricUtil.getConsumeRateMetric().mark();
        return element;
    }
}
