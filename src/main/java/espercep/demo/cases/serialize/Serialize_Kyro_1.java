package espercep.demo.cases.serialize;

import com.alibaba.fastjson.JSONObject;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 * <p>
 * 使用自定义CountWindowGroupState实现count-window的demo
 *
 * @author yitian_song
 */
public class Serialize_Kyro_1 {
    private static final Logger logger = LoggerFactory.getLogger(Serialize_Kyro_1.class);

    public static void main(String[] args) throws Exception {
        int threadNum = 1;
        int eventNum = 500_000;

        CountDownLatch cdt = new CountDownLatch(threadNum);
        for (int i = 0; i < threadNum; ++i) {
            new Thread(() -> {
                try {
                    Kryo kryo = new Kryo();
                    kryo.setRegistrationRequired(false);

                    long cnt = 0L;
                    List<Map> data = new ArrayList<>(eventNum);
                    while (cnt++ < eventNum) {
                        data.add(mock());
                    }

                    // serialize
                    long nanoTime = System.nanoTime();
                    List<String> serialize = new ArrayList<>(data.size());
                    for (Map item : data) {
                        try (ByteArrayOutputStream ostream = new ByteArrayOutputStream()) {
                            ByteBufferOutput output = new ByteBufferOutput(ostream);
                            kryo.writeClassAndObject(output, item);
                            output.close();
                            serialize.add(output.getOutputStream().toString());
                        }
                    }
                    logger.info("Serialization using millis: {}", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - nanoTime));

                    // deserialize
                    nanoTime = System.nanoTime();
                    List<Map> deserialize = new ArrayList<>(data.size());
                    for (String item : serialize) {
                        ByteBufferInput input = new ByteBufferInput(item.getBytes());
                        Object desMap = kryo.readObject(input, JSONObject.class);
                        input.close();
                        deserialize.add((Map) desMap);
                    }
                    logger.info("Deserialization using millis: {}", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - nanoTime));

                } catch (Exception e) {
                    logger.error("Error: ", e);
                } finally {
                    cdt.countDown();
                }
            }).start();
        }
        cdt.await();
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
