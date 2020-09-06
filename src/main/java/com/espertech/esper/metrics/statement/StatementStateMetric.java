package com.espertech.esper.metrics.statement;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * statement状态大小监控指标
 *
 * @author yitian_song 2020/09/06
 */
public abstract class StatementStateMetric {
    private static final Map<String, StatementStateMetric> metrics = new ConcurrentHashMap<>();

    public static DistinctGroupWinStateMetric createDistinctGroupStateMetric(String name) {
        return (DistinctGroupWinStateMetric) metrics.computeIfAbsent(name, key -> new DistinctGroupWinStateMetric());
    }

    public static DistinctWinStateMetric createDistinctStateMetric(String name) {
        return (DistinctWinStateMetric) metrics.computeIfAbsent(name, key -> new DistinctWinStateMetric());
    }

    public static GroupWinStateMetric createGroupStateMetric(String name) {
        return (GroupWinStateMetric) metrics.computeIfAbsent(name, key -> new GroupWinStateMetric());
    }

    public static WinStateMetric createWinStateMetric(String name) {
        return (WinStateMetric) metrics.computeIfAbsent(name, key -> new WinStateMetric());
    }

    public static Map<String, StatementStateMetric> getAllMetrics() {
        return metrics;
    }
}
