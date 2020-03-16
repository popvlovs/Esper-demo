package espercep.demo.util;

import com.codahale.metrics.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/3/11
 */
public class MetricUtil {
    private final static MetricRegistry metrics = new MetricRegistry();
    private static Meter consumeRate;

    private final static Map<String, Metric> metricContainer = new ConcurrentHashMap<>();

    static {
        consumeRate = metrics.meter("Produce rate");
        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(1, TimeUnit.SECONDS);
    }

    public static Meter getConsumeRateMetric() {
        return consumeRate;
    }

    public static Counter getCounter(String name) {
        if (!metricContainer.containsKey(name)) {
            synchronized (metricContainer) {
                if (!metricContainer.containsKey(name)) {
                    metricContainer.put(name, metrics.counter(name));
                }
            }
        }
        return (Counter) metricContainer.get(name);
    }
}
