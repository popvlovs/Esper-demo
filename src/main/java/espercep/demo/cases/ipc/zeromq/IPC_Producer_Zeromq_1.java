package espercep.demo.cases.ipc.zeromq;

import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.Meter;
import espercep.demo.util.ArgsUtil;
import espercep.demo.util.CmdLineOptions;
import espercep.demo.util.MetricUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class IPC_Producer_Zeromq_1 {
    private static final Logger logger = LoggerFactory.getLogger(IPC_Producer_Zeromq_1.class);
    private static CmdLineOptions options;

    public static void main(String[] args) throws Exception {
        options = ArgsUtil.getArg(args);
        logger.info("Using args as {}", options);

        String url = "tcp://127.0.0.1:9191";

        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = context.createSocket(SocketType.PUB);
            socket.bind(url);
            socket.setSndHWM(200000);
            socket.setRcvHWM(200000);
            logger.info("Rcv HWM: {}", socket.getRcvHWM());
            logger.info("Snd HWM: {}", socket.getSndHWM());

            TimeUnit.SECONDS.sleep(1);

            long i = 0L;
            Meter success = MetricUtil.getMeter("Producer success");
            while (++i < options.getEventNum() && !Thread.currentThread().isInterrupted()) {
                socket.send("#1", ZMQ.SNDMORE);
                socket.send(JSONObject.toJSONString(mock()).getBytes());
                success.mark();
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stop producing");
            MetricUtil.reportNow();
        }));
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
