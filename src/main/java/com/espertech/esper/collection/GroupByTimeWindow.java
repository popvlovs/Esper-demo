package com.espertech.esper.collection;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.core.context.util.AgentInstanceContext;
import com.espertech.esper.epl.expression.core.ExprEvaluator;
import com.espertech.esper.epl.expression.core.ExprNode;
import com.espertech.esper.metrics.statement.GroupWinStateMetric;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;

/**
 * 自定义Group by timed-window，满足having-count条件后翻转window，优化内存使用和GC
 */
public final class GroupByTimeWindow extends TimeWindow implements GroupWindow {

    private ExprEvaluator[] groupByEvaluators;
    private ExprNode[] groupByNodes;
    private AgentInstanceContext agentInstanceContext;
    private Map<Object, ArrayDeque<EventBean>> groupedWindow;
    private GroupWinStateMetric metric;

    public GroupByTimeWindow(ExprEvaluator[] groupByEvaluators, ExprNode[] groupByNodes, AgentInstanceContext agentInstanceContext,
                             GroupWinStateMetric metric) {
        super(true);
        this.groupByEvaluators = groupByEvaluators;
        this.groupByNodes = groupByNodes;
        this.agentInstanceContext = agentInstanceContext;
        this.groupedWindow = new HashMap<>();
        this.metric = metric;
    }

    /**
     * Adds event to the time window for the specified timestamp.
     *
     * @param timestamp - the time slot for the event
     * @param bean      - event to add
     */
    public boolean add(long timestamp, EventBean bean) {
        boolean succeed = super.add(timestamp, bean);
        if (succeed) {
            Object groupByKey = getGroupByKey(true, bean);
            ArrayDeque<EventBean> window = groupedWindow.computeIfAbsent(groupByKey, key -> new ArrayDeque<>());
            window.add(bean);
            metric.incGroupWinSize();
            metric.setGroupSize(groupedWindow.size());
        }
        return succeed;
    }

    private void removeFromGroup(EventBean event) {
        Object groupByKey = getGroupByKey(false, event);
        ArrayDeque<EventBean> groupWindow = groupedWindow.get(groupByKey);
        if (groupWindow != null && groupWindow.remove(event)) {
            metric.decGroupWinSize();
            if (groupWindow.isEmpty()) {
                groupedWindow.remove(groupByKey);
            }
        }
    }

    @Override
    public void remove(EventBean theEvent) {
        this.removeFromGroup(theEvent);
        super.remove(theEvent);
    }

    @Override
    public ArrayDeque<EventBean> expireEvents(long expireBefore) {
        ArrayDeque<EventBean> expiredEvents = super.expireEvents(expireBefore);
        if (expiredEvents != null) {
            expiredEvents.forEach(this::removeFromGroup);
        }
        metric.setInnerWinSize(super.getWindowSize());
        return expiredEvents;
    }

    @Override
    public void clearAll() {
        throw new RuntimeException("Unsupported method call");
    }

    @Override
    public void clearAll(Object... groupByKeys) {
        for (Object groupByKey : groupByKeys) {
            ArrayDeque<EventBean> eventBeans = this.groupedWindow.remove(groupByKey);
            if (eventBeans != null) {
                eventBeans.forEach(theEvent -> {
                    super.remove(theEvent);
                    metric.decGroupWinSize();
                });
            }
        }
        super.clearIfEmpty();
    }

    private Object getGroupByKey(boolean isNewData, EventBean... eventBeans) {
        Object[] keys = GroupByKeyUtil.generateGroupKeys(groupByEvaluators, groupByNodes, agentInstanceContext, eventBeans, isNewData);
        if (eventBeans.length == 1) {
            return keys[0];
        } else {
            return keys;
        }
    }
}
