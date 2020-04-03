package espercep.demo.cases.ipc.chronicle;

import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.Meter;
import espercep.demo.util.ArgsUtil;
import espercep.demo.util.CmdLineOptions;
import espercep.demo.util.MetricUtil;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/4/3
 */
public class IPC_Consumer_Chronicle_1 {
    private static final Logger logger = LoggerFactory.getLogger(IPC_Consumer_Chronicle_1.class);
    private static CmdLineOptions options;

    public static void main(String[] args) throws Exception {
        options = ArgsUtil.getArg(args);
        logger.info("Using args as {}", options);

        StoreFileListener storeFileListener = new StoreFileListener() {
            @Override
            public void onReleased(int cycle, File file) {
                try {
                    logger.info("Store file released: {}", file.getCanonicalPath());
                } catch (IOException e) {
                    logger.info("Error on print store file released: ", e);
                }
            }
        };

        try (ChronicleQueue ipc = SingleChronicleQueueBuilder.single("/dev/shm/queue-ipc")
                .rollCycle(RollCycles.SECONDLY_5)
                .storeFileListener(storeFileListener)
                .build()) {
            logger.info("Consumer id: {}", options.getChronicleId());
            ExcerptTailer tailer = ipc.createTailer(options.getChronicleId());
            Meter meter = MetricUtil.getMeter("Consumer");
            while (!Thread.currentThread().isInterrupted()) {
                String text;
                if ((text = tailer.readText()) != null) {
                    // logger.info("Read map: {}", JSONObject.toJSONString(data));
                    JSONObject data = JSONObject.parseObject(text);
                    meter.mark();
                }
            }
        }
    }
}
