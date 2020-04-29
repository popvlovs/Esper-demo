package com.espertech.esper.collection;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.core.context.util.AgentInstanceContext;
import com.espertech.esper.epl.expression.core.ExprEvaluator;
import com.espertech.esper.epl.expression.core.ExprNode;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;

/**
 * 自定义Group by timed-window，满足having-count条件后翻转window，优化内存使用和GC
 */
public class GroupByTimeWindow extends TimeWindow {

    private ExprEvaluator[] groupByEvaluators;
    private ExprNode[] groupByNodes;
    protected AgentInstanceContext agentInstanceContext;
    private Map<Object, ArrayDeque<EventBean>> groupedWindow;

    public GroupByTimeWindow(ExprEvaluator[] groupByEvaluators, ExprNode[] groupByNodes, AgentInstanceContext agentInstanceContext) {
        super(true);
        this.groupByEvaluators = groupByEvaluators;
        this.groupByNodes = groupByNodes;
        this.agentInstanceContext = agentInstanceContext;
        this.groupedWindow = new HashMap<>();
    }

    /**
     * Adds event to the time window for the specified timestamp.
     *
     * @param timestamp - the time slot for the event
     * @param bean      - event to add
     */
    public boolean add(long timestamp, EventBean bean) {
        boolean succeed = super.add(timestamp, bean);
        Object groupByKey = getGroupByKey(true, bean);
        if (groupedWindow.containsKey(groupByKey)) {
            groupedWindow.get(groupByKey).add(bean);
        } else {
            ArrayDeque<EventBean> window = new ArrayDeque<>();
            window.add(bean);
            groupedWindow.put(groupByKey, window);
        }
        return succeed;
    }

    protected void removeFromGroup(EventBean event) {
        Object groupByKey = getGroupByKey(false, event);
        if (groupedWindow.containsKey(groupByKey)) {
            groupedWindow.get(groupByKey).remove(event);
        }
    }

    @Override
    public void remove(EventBean theEvent) {
        this.removeFromGroup(theEvent);
        super.remove(theEvent);
    }

    @Override
    public void clearAll() {
        throw new RuntimeException("Unsupported method call");
    }

    public void clearAll(Object... groupByKeys) {
        for (Object groupByKey : groupByKeys) {
            if (groupedWindow.containsKey(groupByKey)) {
                ArrayDeque<EventBean> eventBeans = this.groupedWindow.remove(groupByKey);
                eventBeans.forEach(super::remove);
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
}
