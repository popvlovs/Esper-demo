package espercep.demo.cases;

import com.alibaba.fastjson.JSONObject;
import com.espertech.esper.client.EPRuntime;
import espercep.demo.util.MetricUtil;

import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.*;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/3/16
 */
public class Queue_Metric_1 {
    public static void main(String[] args) {
        BlockingQueue<Map> queue = new ArrayBlockingQueue<>(1024);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            while (true) {
                if (queue.take() != null) {
                    MetricUtil.getCounter("Consumed").inc();
                }
            }
        });
        sendRandomEvents(queue);
    }

    private static void sendRandomEvents(Queue<Map> queue) {
        long remainingEvents = Long.MAX_VALUE;
        long cnt = 0;
        String[] eventNames = new String[]{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"};
        while (--remainingEvents > 0) {
            int randomVal = new Random().nextInt(eventNames.length);
            JSONObject element = new JSONObject();
            element.put("event_id", cnt++);
            element.put("event_name", eventNames[randomVal % eventNames.length]);
            element.put("src_address", "172.16.100." + cnt % 0xFF);
            element.put("dst_address", "172.16.100." + cnt % 0xFF);
            element.put("occur_time", System.currentTimeMillis());
            MetricUtil.getConsumeRateMetric().mark();
            queue.offer(element);
        }
    }
}
