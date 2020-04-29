package com.espertech.esper.collection;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.core.context.util.AgentInstanceContext;
import com.espertech.esper.epl.expression.core.ExprEvaluator;
import com.espertech.esper.epl.expression.core.ExprNode;
import com.espertech.esper.metrics.instrumentation.InstrumentationHelper;

import java.util.Set;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/4/26
 */
public class GroupByKeyUtil {
    public static Object[] generateGroupKeys(ExprEvaluator[] groupByEvaluators,
                                      ExprNode[] groupByNodes,
                                      AgentInstanceContext agentInstanceContext,
                                      Set<MultiKey<EventBean>> resultSet,
                                      boolean isNewData) {
        if (resultSet.isEmpty()) {
            return null;
        }

        Object[] keys = new Object[resultSet.size()];

        int count = 0;
        for (MultiKey<EventBean> eventsPerStream : resultSet) {
            keys[count] = generateGroupKey(groupByEvaluators, groupByNodes, agentInstanceContext, eventsPerStream.getArray(), isNewData);
            count++;
        }

        return keys;
    }

    public static Object[] generateGroupKeys(ExprEvaluator[] groupByEvaluators,
                                      ExprNode[] groupByNodes,
                                      AgentInstanceContext agentInstanceContext,
                                      EventBean[] events,
                                      boolean isNewData) {
        if (events == null) {
            return null;
        }

        EventBean[] eventsPerStream = new EventBean[1];
        Object[] keys = new Object[events.length];

        for (int i = 0; i < events.length; i++) {
            eventsPerStream[0] = events[i];
            keys[i] = generateGroupKey(groupByEvaluators, groupByNodes, agentInstanceContext, eventsPerStream, isNewData);
        }

        return keys;
    }

    private static Object generateGroupKey(ExprEvaluator[] groupByEvaluators, ExprNode[] groupByNodes, AgentInstanceContext agentInstanceContext, EventBean[] eventsPerStream, boolean isNewData) {
        if (InstrumentationHelper.ENABLED) {
            InstrumentationHelper.get().qResultSetProcessComputeGroupKeys(isNewData, groupByNodes, eventsPerStream);

            Object keyObject;
            if (groupByEvaluators != null && groupByEvaluators.length == 1) {
                keyObject = groupByEvaluators[0].evaluate(eventsPerStream, isNewData, agentInstanceContext);
            } else {
                Object[] keys = new Object[groupByEvaluators.length];
                int count = 0;
                for (ExprEvaluator exprNode : groupByEvaluators) {
                    keys[count] = exprNode.evaluate(eventsPerStream, isNewData, agentInstanceContext);
                    count++;
                }
                keyObject = new MultiKeyUntyped(keys);
            }
            InstrumentationHelper.get().aResultSetProcessComputeGroupKeys(isNewData, keyObject);
            return keyObject;
        }

        if (groupByEvaluators != null && groupByEvaluators.length == 1) {
            return groupByEvaluators[0].evaluate(eventsPerStream, isNewData, agentInstanceContext);
        }

        Object[] keys = new Object[groupByEvaluators.length];
        int count = 0;
        for (ExprEvaluator exprNode : groupByEvaluators) {
            keys[count] = exprNode.evaluate(eventsPerStream, isNewData, agentInstanceContext);
            count++;
        }
        return new MultiKeyUntyped(keys);
    }
}
