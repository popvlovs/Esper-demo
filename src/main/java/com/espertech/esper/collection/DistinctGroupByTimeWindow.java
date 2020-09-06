package com.espertech.esper.collection;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.core.context.util.AgentInstanceContext;
import com.espertech.esper.epl.agg.service.AggregationService;
import com.espertech.esper.epl.expression.core.ExprEvaluator;
import com.espertech.esper.epl.expression.core.ExprNode;
import com.espertech.esper.metrics.statement.DistinctGroupWinStateMetric;

import java.util.*;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * having count(distinct(xx)) + group by
 * 该时间窗口按distinct和group by对窗口中事件进行分区，每个分区最多只保留 stackSize 条记录
 * 以实现内存优化
 *
 * @author yitian_song 2020/4/28
 */
public final class DistinctGroupByTimeWindow extends TimeWindow implements GroupWindow {
    private ExprEvaluator[] groupByEvaluators;
    private ExprNode[] groupByNodes;
    private ExprNode distinctByNode;
    private ExprEvaluator distinctByEvaluator;
    private AgentInstanceContext agentInstanceContext;
    private Map<Object, DistinctWindowPair> groupedWindow;
    private int stackSize;
    private DistinctGroupWinStateMetric metric;

    /**
     * 当distinct stack已满，是否直接拒绝最新的event
     * 或者从状态中移除一个旧的event (aggregator & window)，以装入新的event
     * 直接拒绝的话吞吐量会比较高，但是可能导致
     */
    private boolean isDiscardNewEvent;

    public DistinctGroupByTimeWindow(ExprEvaluator[] groupByEvaluators, ExprNode[] groupByNodes,
                                     AgentInstanceContext agentInstanceContext, ExprNode distinctByNode, int stackSize,
                                     DistinctGroupWinStateMetric metric) {
        super(true);
        this.groupByEvaluators = groupByEvaluators;
        this.groupByNodes = groupByNodes;
        this.distinctByNode = distinctByNode;
        this.distinctByEvaluator = this.distinctByNode.getExprEvaluator();
        this.agentInstanceContext = agentInstanceContext;
        this.stackSize = stackSize;
        this.groupedWindow = new HashMap<>();
        this.metric = metric;
    }

    @Override
    public boolean add(long timestamp, EventBean bean) {
        boolean succeed = super.add(timestamp, bean);
        if (succeed) {
            Object groupByKey = getGroupByKey(true, bean);
            DistinctWindowPair pairPerGroup = groupedWindow.computeIfAbsent(groupByKey, key -> new DistinctWindowPair(this));
            if (!pairPerGroup.add(bean)) {
                succeed = false;
                super.remove(bean);
            }
        }
        return succeed;
    }

    @Override
    public void remove(EventBean theEvent) {
        this.removeFromGroup(theEvent);
        super.remove(theEvent);
    }

    @Override
    public ArrayDeque<EventBean> expireEvents(long expireBefore) {
        ArrayDeque<EventBean> expireEvents = super.expireEvents(expireBefore);
        if (expireEvents != null) {
            expireEvents.forEach(this::removeFromGroup);
        }
        metric.setInnerWinSize(super.getWindowSize());
        return expireEvents;
    }

    private void removeFromGroup(EventBean theEvent) {
        Object groupByKey = getGroupByKey(false, theEvent);
        DistinctWindowPair groupWindow = groupedWindow.get(groupByKey);
        if (groupWindow != null) {
            groupWindow.remove(theEvent);
        }
    }

    private void removeWindow(EventBean theEvent) {
        super.remove(theEvent);
    }

    private Object getDistinctValue(boolean isNewData, EventBean... eventBeans) {
        return distinctByEvaluator.evaluate(eventBeans, isNewData, this.agentInstanceContext);
    }

    @Override
    public void clearAll() {
        throw new RuntimeException("Unsupported method call");
    }

    @Override
    public void clearAll(Object... groupByKeys) {
        for (Object groupByKey : groupByKeys) {
            DistinctWindowPair distinctWindowPair = this.groupedWindow.remove(groupByKey);
            if (distinctWindowPair != null) {
                distinctWindowPair.getWindow().forEach(theEvent -> {
                    super.remove(theEvent);
                    metric.decGroupWinSize();
                });
            }
        }
    }

    private Object getGroupByKey(boolean isNewData, EventBean... eventBeans) {
        Object[] keys = GroupByKeyUtil.generateGroupKeys(groupByEvaluators, groupByNodes, agentInstanceContext, eventBeans, isNewData);
        if (eventBeans.length == 1) {
            return keys[0];
        } else {
            return keys;
        }
    }

    private class DistinctWindowPair {
        private Set<EventBean> window;
        private Map<Object, ArrayDeque<EventBean>> distinctByStack;
        private DistinctGroupByTimeWindow parent;

        public DistinctWindowPair(DistinctGroupByTimeWindow parent) {
            this.window = new HashSet<>();
            this.distinctByStack = new HashMap<>();
            this.parent = parent;
        }

        public Collection<EventBean> getWindow() {
            return window;
        }

        public boolean add(EventBean bean) {
            Object distinctValue = getDistinctValue(true, bean);

            ArrayDeque<EventBean> stack = distinctByStack.computeIfAbsent(distinctValue, key -> new ArrayDeque<>(stackSize));
            if (stack.size() < stackSize) {
                stack.offer(bean);
                this.window.add(bean);
                metric.incGroupWinSize();
            } else {
                if (isDiscardNewEvent) {
                    return false;
                } else {
                    EventBean eventToExpire = stack.poll();
                    this.window.remove(eventToExpire);
                    this.removeFromAggregator(eventToExpire);
                    this.parent.removeWindow(eventToExpire);
                    stack.offer(bean);
                }
            }
            return true;
        }

        public void remove(EventBean bean) {
            Object distinctValue = getDistinctValue(false, bean);
            if (this.window.remove(bean)) {
                metric.decGroupWinSize();
            }
            ArrayDeque<EventBean> stack = distinctByStack.get(distinctValue);
            if (stack != null) {
                stack.remove(bean);
            }
        }

        private void removeFromAggregator(EventBean... eventBeans) {
            Set<AggregationService> aggregationServiceSet = agentInstanceContext.getAggregationServices();
            if (aggregationServiceSet.size() == 1) {
                for (AggregationService aggregationService : aggregationServiceSet) {
                    aggregationService.applyLeave(eventBeans, getGroupByKey(false, eventBeans), agentInstanceContext);
                }
            } else {
                throw new RuntimeException("Unexpected AggregationService size in AgentInstanceContext");
            }
        }
    }
}
