package espercep.demo.mock;

import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.espertech.esper.client.EPRuntime;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class MockDataThread extends Thread {

    private static Meter consumeRate;

    static {
        final MetricRegistry metrics = new MetricRegistry();
        consumeRate = metrics.meter("Produce rate");
        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(1, TimeUnit.SECONDS);
    }

    private int remainingEvents;
    private EPRuntime esperRuntime;
    private int ratio;
    private static volatile Integer cnt = 0;
    private volatile long startTime = System.currentTimeMillis();

    public MockDataThread(int remainingEvents, int ratio, EPRuntime esperRuntime) {
        this.remainingEvents = remainingEvents;
        this.esperRuntime = esperRuntime;
        this.ratio = ratio;
    }

    @Override
    public void run() {
        while (--remainingEvents >= 0) {
            JSONObject element = getElement();
            esperRuntime.sendEvent(element, "TestEvent");
        }
    }

    private JSONObject getElement() {
        JSONObject element = new JSONObject();
        element.put("event_id", cnt++);
        element.put("event_name", (cnt%ratio == 0) ? "邮件发送" : "邮件登陆");
        element.put("src_address", "172.16.100." + cnt%0xFF);
        element.put("dst_address", "172.16.100." + cnt%0xFF);
        // 天坑：这里写startTime + cnt*1000的话会有memory leak，写成startTime + cnt*1000L的话就Ok
        // 原因未知，不接esper也没事，只有接了esper才会触发
        // element.put("occur_time", startTime + cnt*1000L);
        element.put("occur_time", System.currentTimeMillis());
        element.put("payload", "{\"nta\":\"1\",\"app_protocol\":\"dns\",\"threat_rule_id\":\"2027863\",\"receive_time\":1572379271341,\"collector_source\":\"Hansight-NTA\",\"event_level\":0,\"occur_time\":1574647014096,\"dst_address\":\"8.8.4.4\",\"threat_feature\":\".P...........smarttender.biz.....\",\"src_port\":51944,\"event_name\":\"网络连接\",\"sensor_id\":\"16279cde-adb9-4e97-9efc-cb4cb756074a\",\"end_time\":1572379244327,\"rule_id\":\"72190fa7-2a8d-457d-8d83-4985ac8c9b48\",\"dst_port\":53,\"threat_info\":\"EXPLOIT.AAC\",\"response\":\"allowed\",\"id\":\"11883905637720065\",\"event_digest\":\"nta_alert\",\"src_mac\":\"7C:1E:06:DF:3E:01\",\"event_type\":\"/14YRL6KY0003\",\"tran_protocol\":17,\"tx_id\":19,\"first_time\":1572379244325,\"in_iface\":\"enp7s0\",\"src_address\":\"172.16.104.58\",\"rule_name\":\"nta_dispatcher\",\"unit\":\"bytes\",\"log_type\":\"Potentially Bad Traffic\",\"flow_id\":195281868879527,\"dev_address\":\"172.16.100.127\",\"original_type\":\"YWxlcnQgZG5zICRIT01FX05FVCBhbnkgLT4gYW55IGFueSAobXNnOiJJTkZPIE9ic2VydmVkIEROUyBRdWVyeSB0byAuYml6IFRMRCI7IGRuc19xdWVyeTsgY29udGVudDoiLmJpeiI7IG5vY2FzZTsgaXNkYXRhYXQ6ITEscmVsYXRpdmU7IG1ldGFkYXRhOiBmb3JtZXJfY2F0ZWdvcnkgSU5GTzsgcmVmZXJlbmNlOnVybCx3d3cuc3BhbWhhdXMub3JnL3N0YXRpc3RpY3MvdGxkcy87IGNsYXNzdHlwZTpiYWQtdW5rbm93bjsgc2lkOjIwMjc4NjM7IHJldjoyOyBtZXRhZGF0YTphZmZlY3RlZF9wcm9kdWN0IEFueSwgYXR0YWNrX3RhcmdldCBDbGllbnRfRW5kcG9pbnQsIGRlcGxveW1lbnQgUGVyaW1ldGVyLCBzaWduYXR1cmVfc2V2ZXJpdHkgTWFqb3IsIGNyZWF0ZWRfYXQgMjAxOV8wOF8xMywgdXBkYXRlZF9hdCAyMDE5XzA5XzI4OykN\",\"vendor\":\"NTA（HanSight）\",\"data_source\":\"NTA（HanSight）\",\"dst_mac\":\"74:85:C4:EA:00:52\",\"original_info\":\"vlABAAABAAAAAAAAC3NtYXJ0dGVuZGVyA2JpegAAAQAB\",\"protocol\":\"alert\",\"in_iface_2\":\"enp7s0\"}");
        consumeRate.mark();
        return element;
    }

    public void await() {
        final CountDownLatch cdt = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(cdt::countDown));
        try {
            cdt.await();
        } catch (InterruptedException e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
