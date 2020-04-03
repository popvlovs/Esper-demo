package espercep.demo.cases.ipc.zeromq;

import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.Meter;
import espercep.demo.util.MetricUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.concurrent.TimeUnit;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class IPC_Consumer_Zeromq_1 {
    private static final Logger logger = LoggerFactory.getLogger(IPC_Consumer_Zeromq_1.class);

    public static void main(String[] args) throws Exception {

        String url = "tcp://127.0.0.1:9191";

        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = context.createSocket(SocketType.SUB);
            socket.connect(url);
            socket.subscribe("#1");
            socket.setSndHWM(200000);
            socket.setRcvHWM(200000);
            logger.info("Rcv HWM: {}", socket.getRcvHWM());
            logger.info("Snd HWM: {}", socket.getSndHWM());

            logger.info("Start to receive message from {}", url);
            Meter meter = MetricUtil.getMeter("Consumer #1");
            while (!Thread.currentThread().isInterrupted()) {
                socket.recvStr(0);
                String message = socket.recvStr(0);
                //TimeUnit.MILLISECONDS.sleep(1);
                JSONObject data = JSONObject.parseObject(message);
                meter.mark();
            }
        }
        logger.info("Stop consuming");
        MetricUtil.reportNow();
        System.exit(0);
    }
}
