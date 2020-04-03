package espercep.demo.cases.ipc.nanomsg;

import espercep.demo.util.MetricUtil;
import nanomsg.pubsub.SubSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class IPC_Consumer_Nanomsg_1 {
    private static final Logger logger = LoggerFactory.getLogger(IPC_Consumer_Nanomsg_1.class);

    public static void main(String[] args) throws Exception {
        String url = "ipc://127.0.0.1:9191";
        final SubSocket socket = new SubSocket();
        socket.connect(url);
        socket.subscribe("#1");

        while (!Thread.currentThread().isInterrupted()) {
            String message = socket.recvString();
            MetricUtil.getMeter("Consumer").mark();
        }
        socket.close();
        System.exit(0);
    }
}
