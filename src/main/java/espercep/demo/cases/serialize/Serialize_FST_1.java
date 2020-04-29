package espercep.demo.cases.serialize;

import com.alibaba.fastjson.JSONObject;
import espercep.demo.util.FSTUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/4/7
 */
public class Serialize_FST_1 {

    private static final Logger logger = LoggerFactory.getLogger(Serialize_FST_1.class);

    public static void main(String[] args) {
        int eventNum = 1000_000;

        long cnt = 0L;
        List<Map> data = new ArrayList<>(eventNum);
        while (cnt++ < eventNum) {
            data.add(mock());
        }
        logger.info("Start to test serialize/deserialize perf");
        long nanoTime = System.nanoTime();
        List<byte[]> serialize = new ArrayList<>(data.size());
        for (Map item : data) {
            serialize.add(FSTUtil.serialize(item));
        }
        logger.info("Serialization using millis: {}", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - nanoTime));

        nanoTime = System.nanoTime();
        List<Map> deserialize = new ArrayList<>(data.size());
        for (byte[] item : serialize) {
            deserialize.add(FSTUtil.deserialize(item));
        }
        logger.info("Deserialization using millis: {}", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - nanoTime));
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
