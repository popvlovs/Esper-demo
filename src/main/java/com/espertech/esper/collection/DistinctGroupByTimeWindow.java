package com.espertech.esper.collection;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.core.context.util.AgentInstanceContext;
import com.espertech.esper.epl.agg.service.AggregationService;
import com.espertech.esper.epl.expression.core.ExprEvaluator;
import com.espertech.esper.epl.expression.core.ExprNode;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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

    public DistinctGroupByTimeWindow(ExprEvaluator[] groupByEvaluators, ExprNode[] groupByNodes,
                                     AgentInstanceContext agentInstanceContext, ExprNode distinctByNode, int stackSize) {
        super(true);
        this.groupByEvaluators = groupByEvaluators;
        this.groupByNodes = groupByNodes;
        this.distinctByNode = distinctByNode;
        this.distinctByEvaluator = this.distinctByNode.getExprEvaluator();
        this.agentInstanceContext = agentInstanceContext;
        this.stackSize = stackSize;
        this.groupedWindow = new HashMap<>();
    }

    @Override
    public boolean add(long timestamp, EventBean bean) {
        boolean succeed = super.add(timestamp, bean);
        Object groupByKey = getGroupByKey(true, bean);
        if (groupedWindow.containsKey(groupByKey)) {
            groupedWindow.get(groupByKey).add(bean);
        } else {
            DistinctWindowPair pairPerGroup = new DistinctWindowPair(this);
            pairPerGroup.add(bean);
            groupedWindow.put(groupByKey, pairPerGroup);
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
        return expireEvents;
    }

    private void removeFromGroup(EventBean theEvent) {
        Object groupByKey = getGroupByKey(false, theEvent);
        if (groupedWindow.containsKey(groupByKey)) {
            groupedWindow.get(groupByKey).remove(theEvent);
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
            if (groupedWindow.containsKey(groupByKey)) {
                DistinctWindowPair distinctWindowPair = this.groupedWindow.remove(groupByKey);
                distinctWindowPair.getWindow().forEach(super::remove);
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
        private ArrayDeque<EventBean> window;
        private Map<Object, ArrayDeque<EventBean>> distinctByStack;
        private DistinctGroupByTimeWindow parent;

        public DistinctWindowPair(DistinctGroupByTimeWindow parent) {
            this.window = new ArrayDeque<>();
            this.distinctByStack = new HashMap<>();
            this.parent = parent;
        }

        public ArrayDeque<EventBean> getWindow() {
            return window;
        }

        public void add(EventBean bean) {
            Object distinctValue = getDistinctValue(true, bean);
            ArrayDeque<EventBean> stack;
            this.window.add(bean);
            if (distinctByStack.containsKey(distinctValue)) {
                stack = distinctByStack.get(distinctValue);
            } else {
                stack = new ArrayDeque<>(stackSize);
                distinctByStack.put(distinctValue, stack);
            }
            if (stack.size() < stackSize) {
                stack.push(bean);
            } else {
                EventBean eventToExpire = stack.pop();
                this.window.remove(bean);
                this.removeFromAggregator(eventToExpire);
                this.parent.removeWindow(eventToExpire);
                stack.push(bean);
            }
        }

        public void remove(EventBean bean) {
            Object distinctValue = getDistinctValue(false, bean);
            this.window.remove(bean);
            if (distinctByStack.containsKey(distinctValue)) {
                ArrayDeque<EventBean> stack = distinctByStack.get(distinctValue);
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
