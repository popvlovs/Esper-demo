package espercep.demo;

import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.*;
import espercep.demo.reorder.OrderedQueue;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class OrderQueueTest {
    public static void main(String[] args) {
        new OutOfOrderQueueThread(100000000, 10).start();
    }

    private static class OutOfOrderQueueThread extends Thread {

        private Meter consumeRate;
        private Counter outputs;
        private Gauge queueSize;

        private int remainingEvents;
        private int ratio;
        private volatile Integer cnt = 0;
        private OrderedQueue queue;

        public OutOfOrderQueueThread(int remainingEvents, int ratio) {
            this.remainingEvents = remainingEvents;
            this.ratio = ratio;
            //this.queue = OrderedQueue.onEventDeque(System.out::println);
            this.queue = OrderedQueue.onEventDeque(event -> {
                outputs.inc();
            });
            final MetricRegistry metrics = new MetricRegistry();
            consumeRate = metrics.meter("Produce rate");
            outputs = metrics.counter("Output count");
            queueSize = metrics.gauge("Queue size", () -> queue::getQueueSizeCnt);
            ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build();
            reporter.start(1, TimeUnit.SECONDS);
        }

        @Override
        public void run() {
            while (--remainingEvents > 0) {
                JSONObject element = getElement();
                this.queue.bufferEvent(element, element.getLongValue("occur_time"));
            }
        }

        private JSONObject getElement() {
            JSONObject element = new JSONObject();
            element.put("event_id", cnt++);
            element.put("event_name", (cnt % ratio == 0) ? "邮件发送" : "邮件登陆");
            element.put("src_address", "172.16.100." + cnt % 0xFF);
            element.put("dst_address", "172.16.100." + cnt % 0xFF);
            element.put("occur_time", System.currentTimeMillis() + new Random().nextInt(2000) - 1000L);
            consumeRate.mark();
            return element;
        }
    }
}
