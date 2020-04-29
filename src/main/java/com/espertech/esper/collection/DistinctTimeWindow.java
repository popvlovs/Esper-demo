package com.espertech.esper.collection;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.core.context.util.AgentInstanceContext;
import com.espertech.esper.epl.expression.core.ExprEvaluator;
import com.espertech.esper.epl.expression.core.ExprNode;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/4/28
 */
public class DistinctTimeWindow extends TimeWindow {
    private ExprNode distinctByNode;
    private ExprEvaluator distinctByEvaluator;
    private AgentInstanceContext agentInstanceContext;
    private Map<Object, ArrayDeque<EventBean>> distinctByStack;
    private int stackSize;

    public DistinctTimeWindow(AgentInstanceContext agentInstanceContext, ExprNode distinctByNode, int stackSize) {
        super(true);
        this.distinctByNode = distinctByNode;
        this.distinctByEvaluator = this.distinctByNode.getExprEvaluator();
        this.agentInstanceContext = agentInstanceContext;
        this.distinctByStack = new HashMap<>();
        this.stackSize = stackSize;
    }

    @Override
    public boolean add(long timestamp, EventBean bean) {
        boolean succeed = super.add(timestamp, bean);
        Object distinctValue = getDistinctValue(true, bean);
        ArrayDeque<EventBean> stack;
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
            super.remove(eventToExpire);
            stack.push(bean);
        }
        return succeed;
    }

    @Override
    public void remove(EventBean theEvent) {
        Object distinctValue = getDistinctValue(false, theEvent);
        ArrayDeque<EventBean> stack;
        if (distinctByStack.containsKey(distinctValue)) {
            stack = distinctByStack.get(distinctValue);
            stack.remove(theEvent);
        }
        super.remove(theEvent);
    }

    private Object getDistinctValue(boolean isNewData, EventBean... eventBeans) {
        return distinctByEvaluator.evaluate(eventBeans, isNewData, this.agentInstanceContext);
    }
}
