package espercep.demo.util;

import com.codahale.metrics.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/3/11
 */
public class MetricUtil {
    private final static MetricRegistry metrics = new MetricRegistry();
    private static Meter consumeRate;
    private static AtomicBoolean enabled = new AtomicBoolean(false);

    private final static Map<String, Metric> metricContainer = new ConcurrentHashMap<>();
    private static ConsoleReporter reporter;

    static {
        reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        disable(false);
    }

    public static void disable(boolean disable) {
        if (enabled.compareAndSet(disable, !disable)) {
            if (disable) {
                reporter.stop();
            } else {
                reporter.start(1, TimeUnit.SECONDS);
            }
        }
    }

    public static Meter getConsumeRateMetric() {
        if (!enabled.get()) {
            return NoMeter.getInstance();
        }
        if (consumeRate != null) {
            return consumeRate;
        } else {
            return (consumeRate = metrics.meter("Produce rate"));
        }
    }

    public static void reportNow() {
        reporter.report();
    }

    public static Timer getTimer(String name) {
        if (!metricContainer.containsKey(name)) {
            synchronized (metricContainer) {
                if (!metricContainer.containsKey(name)) {
                    metricContainer.put(name, metrics.timer(name));
                }
            }
        }
        return (Timer) metricContainer.get(name);
    }

    public static Histogram getHistogram(String name) {
        if (!metricContainer.containsKey(name)) {
            synchronized (metricContainer) {
                if (!metricContainer.containsKey(name)) {
                    metricContainer.put(name, metrics.histogram(name));
                }
            }
        }
        return (Histogram) metricContainer.get(name);
    }

    public static Counter getCounter(String name) {
        if (!enabled.get()) {
            return NoCounter.getInstance();
        }
        if (!metricContainer.containsKey(name)) {
            synchronized (metricContainer) {
                if (!metricContainer.containsKey(name)) {
                    metricContainer.put(name, metrics.counter(name));
                }
            }
        }
        return (Counter) metricContainer.get(name);
    }

    public static Meter getMeter(String name) {
        if (!enabled.get()) {
            return NoMeter.getInstance();
        }
        if (!metricContainer.containsKey(name)) {
            synchronized (metricContainer) {
                if (!metricContainer.containsKey(name)) {
                    metricContainer.put(name, metrics.meter(name));
                }
            }
        }
        return (Meter) metricContainer.get(name);
    }

    private static class NoCounter extends Counter {
        private static final NoCounter instance = new NoCounter();

        public static NoCounter getInstance() {
            return instance;
        }

        private NoCounter() {
        }

        @Override
        public void inc() {
        }

        @Override
        public void inc(long n) {
        }
    }

    private static class NoMeter extends Meter {
        private static final NoMeter instance = new NoMeter();

        public static NoMeter getInstance() {
            return instance;
        }

        private NoMeter() {
        }

        @Override
        public void mark() {
        }

        @Override
        public void mark(long n) {
        }
    }
}
