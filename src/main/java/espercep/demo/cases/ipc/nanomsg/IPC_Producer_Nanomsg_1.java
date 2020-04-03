package espercep.demo.cases.ipc.nanomsg;

import com.alibaba.fastjson.JSONObject;
import espercep.demo.util.MetricUtil;
import nanomsg.pubsub.PubSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class IPC_Producer_Nanomsg_1 {
    private static final Logger logger = LoggerFactory.getLogger(IPC_Producer_Nanomsg_1.class);

    public static void main(String[] args) throws Exception {
        String url = "ipc://127.0.0.1:9191";
        final PubSocket socket = new PubSocket();
        socket.bind(url);

        long i = 0L;
        while (++i < Long.MAX_VALUE && !Thread.currentThread().isInterrupted()) {
            socket.send(JSONObject.toJSONString(mock()).getBytes());
            MetricUtil.getMeter("Producer").mark();
        }
        socket.close();
        System.exit(0);
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
