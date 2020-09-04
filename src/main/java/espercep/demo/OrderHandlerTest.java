package espercep.demo;

import com.alibaba.fastjson.JSONObject;
import espercep.demo.reorder.SequenceOrderHandler;
import espercep.demo.util.MetricUtil;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class OrderHandlerTest {
    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(10);
        SequenceOrderHandler handler = new SequenceOrderHandler(3000L, 1_800_000L, 3000L, "occur_time");
        handler.start(event -> MetricUtil.getMeter("out-of-order-output-rate").mark());

        for (int i = 0; i < 10; i++) {
            executor.submit(new OutOfOrderQueueThread(Integer.MAX_VALUE, 10, handler));
        }
    }

    private static class OutOfOrderQueueThread extends Thread {

        private int remainingEvents;
        private int ratio;
        private volatile Integer cnt = 0;
        private SequenceOrderHandler handler;

        public OutOfOrderQueueThread(int remainingEvents, int ratio, SequenceOrderHandler handler) {
            this.remainingEvents = remainingEvents;
            this.ratio = ratio;
            this.handler = handler;
        }

        @Override
        public void run() {
            while (--remainingEvents > 0) {
                JSONObject element = getElement();
                this.handler.bufferEvent(element);
            }
        }

        private JSONObject getElement() {
            JSONObject element = new JSONObject();
            element.put("event_id", cnt++);
            element.put("event_name", (cnt % ratio == 0) ? "邮件发送" : "邮件登陆");
            element.put("src_address", "172.16.100." + cnt % 0xFF);
            element.put("dst_address", "172.16.100." + cnt % 0xFF);
            element.put("occur_time", System.currentTimeMillis() + new Random().nextInt(2000) - 1000L);
            return element;
        }
    }
}
