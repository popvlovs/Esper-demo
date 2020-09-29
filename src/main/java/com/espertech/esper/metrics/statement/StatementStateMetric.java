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

    private String name;

    public StatementStateMetric(String name) {
        this.name = name;
    }

    protected abstract StatementStateMetric clear();

    public static DistinctGroupWinStateMetric createDistinctGroupStateMetric(String name) {
        return (DistinctGroupWinStateMetric) metrics.computeIfAbsent(name, DistinctGroupWinStateMetric::new).clear();
    }

    public static DistinctWinStateMetric createDistinctStateMetric(String name) {
        return (DistinctWinStateMetric) metrics.computeIfAbsent(name, DistinctWinStateMetric::new).clear();
    }

    public static GroupWinStateMetric createGroupStateMetric(String name) {
        return (GroupWinStateMetric) metrics.computeIfAbsent(name, GroupWinStateMetric::new).clear();
    }

    public static WinStateMetric createWinStateMetric(String name) {
        return (WinStateMetric) metrics.computeIfAbsent(name, WinStateMetric::new).clear();
    }

    public static PatternStateMetric getPatternStateMetric(String name) {
        StatementStateMetric metric = metrics.computeIfAbsent(name, PatternStateMetric::new);
        if (metric instanceof PatternStateMetric) {
            return (PatternStateMetric) metric;
        } else {
            return null;
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public static Map<String, StatementStateMetric> getAllMetrics() {
        return metrics;
    }
}
