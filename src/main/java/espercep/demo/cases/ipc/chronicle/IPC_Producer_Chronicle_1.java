package espercep.demo.cases.ipc.chronicle;

import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.Meter;
import espercep.demo.util.ArgsUtil;
import espercep.demo.util.CmdLineOptions;
import espercep.demo.util.MetricUtil;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/4/3
 */
public class IPC_Producer_Chronicle_1 {
    private static final Logger logger = LoggerFactory.getLogger(IPC_Producer_Chronicle_1.class);
    private static CmdLineOptions options;

    public static void main(String[] args) throws Exception {
        options = ArgsUtil.getArg(args);
        logger.info("Using args as {}", options);
        StoreFileListener storeFileListener = new StoreFileListener() {
            @Override
            public void onReleased(int cycle, File file) {
                try {
                    logger.info("Store file released: {}", file.getCanonicalPath());
                    File[] queueFiles = file.getParentFile().listFiles((dir, filename) -> filename.endsWith("cq4"));
                    Arrays.sort(queueFiles, Comparator.comparingLong(File::lastModified).reversed());
                    for (int i = 5; i < queueFiles.length; ++i) {
                        boolean succeed = queueFiles[i].delete();
                        if (succeed) {
                            logger.info("Delete released store file success: {}", queueFiles[i].getCanonicalPath());
                        } else {
                            logger.info("Delete released store file fail: {}", queueFiles[i].getCanonicalPath());
                        }
                    }
                } catch (IOException e) {
                    logger.info("Error on print store file released: ", e);
                }
            }
        };

        Meter meter = MetricUtil.getMeter("Producer");
        try (ChronicleQueue ipc = SingleChronicleQueueBuilder.single("/dev/shm/queue-ipc")
                .rollCycle(RollCycles.SECONDLY_5)
                .storeFileListener(storeFileListener)
                .build()) {
            ExcerptAppender appender = ipc.acquireAppender();
            while (!Thread.currentThread().isInterrupted() && cnt.get() < options.getEventNum()) {
                String data = JSONObject.toJSONString(mock());
                appender.writeText(data);
                meter.mark();
            }
        }
    }

    private static ThreadLocal<Long> cnt = ThreadLocal.withInitial(() -> 0L);

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
        return element;
    }
}
