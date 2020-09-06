/*
 ***************************************************************************************
 *  Copyright (C) 2006 EsperTech, Inc. All rights reserved.                            *
 *  http://www.espertech.com/esper                                                     *
 *  http://www.espertech.com                                                           *
 *  ---------------------------------------------------------------------------------- *
 *  The software in this package is published under the terms of the GPL license       *
 *  a copy of which has been included with this distribution in the license.txt file.  *
 ***************************************************************************************
 */
package com.espertech.esper.view.window;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventType;
import com.espertech.esper.collection.*;
import com.espertech.esper.core.context.util.AgentInstanceContext;
import com.espertech.esper.core.context.util.AgentInstanceViewFactoryChainContext;
import com.espertech.esper.epl.expression.core.ExprEvaluator;
import com.espertech.esper.epl.expression.core.ExprIdentNode;
import com.espertech.esper.epl.expression.core.ExprNode;
import com.espertech.esper.epl.expression.core.ExprNodeUtility;
import com.espertech.esper.epl.expression.methodagg.ExprCountNode;
import com.espertech.esper.epl.expression.time.ExprTimePeriodEvalDeltaConst;
import com.espertech.esper.epl.spec.GroupByClauseExpressions;
import com.espertech.esper.metrics.instrumentation.InstrumentationHelper;
import com.espertech.esper.metrics.statement.*;
import com.espertech.esper.util.CollectionUtil;
import com.espertech.esper.util.FeatureToggle;
import com.espertech.esper.view.*;

import java.util.ArrayDeque;
import java.util.Iterator;

/**
 * View for a moving window extending the specified amount of time into the past, driven entirely by external timing
 * supplied within long-type timestamp values in a field of the event beans that the view receives.
 * <p>
 * The view is completely driven by timestamp values that are supplied by the events it receives,
 * and does not use the schedule service time.
 * It requires a field name as parameter for a field that returns ascending long-type timestamp values.
 * It also requires a long-type parameter setting the time length in milliseconds of the time window.
 * Events are expected to provide long-type timestamp values in natural order. The view does
 * itself not use the current system time for keeping track of the time window, but just the
 * timestamp values supplied by the events sent in.
 * <p>
 * The arrival of new events with a newer timestamp then past events causes the window to be re-evaluated and the oldest
 * events pushed out of the window. Ie. Assume event X1 with timestamp T1 is in the window.
 * When event Xn with timestamp Tn arrives, and the window time length in milliseconds is t, then if
 * ((Tn - T1) &gt; t == true) then event X1 is pushed as oldData out of the window. It is assumed that
 * events are sent in in their natural order and the timestamp values are ascending.
 */
public class ExternallyTimedWindowView extends ViewSupport implements DataWindowView, CloneableView {
    private final ExternallyTimedWindowViewFactory externallyTimedWindowViewFactory;
    private final ExprNode timestampExpression;
    private final ExprEvaluator timestampExpressionEval;
    private final ExprTimePeriodEvalDeltaConst timeDeltaComputation;

    private final EventBean[] eventsPerStream = new EventBean[1];
    protected TimeWindow timeWindow;
    private ViewUpdatedCollection viewUpdatedCollection;
    protected AgentInstanceViewFactoryChainContext agentInstanceViewFactoryContext;

    /**
     * 用于标记externally timed window是否包含groupBy
     * 如果没有则允许在触发having count后
     * 不仅重置aggregator state，也清空window的状态
     */
    private boolean isGroupedExtTimeWindow;

    /**
     * Constructor.
     *
     * @param timestampExpression              is the field name containing a long timestamp value
     *                                         that should be in ascending order for the natural order of events and is intended to reflect
     *                                         System.currentTimeInMillis but does not necessarily have to.
     *                                         out of the window as oldData in the update method. The view compares
     *                                         each events timestamp against the newest event timestamp and those with a delta
     *                                         greater then secondsBeforeExpiry are pushed out of the window.
     * @param viewUpdatedCollection            is a collection that the view must update when receiving events
     * @param externallyTimedWindowViewFactory for copying this view in a group-by
     * @param agentInstanceViewFactoryContext  context for expression evalauation
     * @param timeDeltaComputation             delta computation
     * @param timestampExpressionEval          timestamp expr eval
     */
    public ExternallyTimedWindowView(ExternallyTimedWindowViewFactory externallyTimedWindowViewFactory,
                                     ExprNode timestampExpression, ExprEvaluator timestampExpressionEval,
                                     ExprTimePeriodEvalDeltaConst timeDeltaComputation, ViewUpdatedCollection viewUpdatedCollection,
                                     AgentInstanceViewFactoryChainContext agentInstanceViewFactoryContext) {
        this.externallyTimedWindowViewFactory = externallyTimedWindowViewFactory;
        this.timestampExpression = timestampExpression;
        this.timestampExpressionEval = timestampExpressionEval;
        this.timeDeltaComputation = timeDeltaComputation;
        this.viewUpdatedCollection = viewUpdatedCollection;
        this.agentInstanceViewFactoryContext = agentInstanceViewFactoryContext;

        AgentInstanceContext agentInstanceContext = this.agentInstanceViewFactoryContext.getAgentInstanceContext();

        String statementName = agentInstanceContext.getStatementName();

        if (agentInstanceContext != null) {
            agentInstanceContext.addExtTimedWindowView(this);
            ExprNode distinctByNode = getHavingDistinctNodes(agentInstanceContext);
            ExprNode[] groupByNodes = getGroupByNodes(agentInstanceContext);
            if (groupByNodes != null && groupByNodes.length > 0) {
                ExprEvaluator[] groupByEvaluators = ExprNodeUtility.getEvaluators(groupByNodes);
                if (FeatureToggle.isDiscardExtTimedWindowOnAggOutput()) {
                    isGroupedExtTimeWindow = true;
                    // Use grouped time-window
                    if (distinctByNode != null && FeatureToggle.isReduceDistinctEventNum()) {
                        // Reduce distinct events of window (retain top-k + last)
                        DistinctGroupWinStateMetric metric = StatementStateMetric.createDistinctGroupStateMetric(statementName);
                        this.timeWindow = new DistinctGroupByTimeWindow(groupByEvaluators, groupByNodes, agentInstanceContext, distinctByNode, FeatureToggle.getNumDistinctEventRetained(), metric);
                    } else {
                        GroupWinStateMetric metric = StatementStateMetric.createGroupStateMetric(statementName);
                        this.timeWindow = new GroupByTimeWindow(groupByEvaluators, groupByNodes, agentInstanceContext, metric);
                    }
                }
            }
            if (this.timeWindow == null) {
                if (distinctByNode != null && FeatureToggle.isReduceDistinctEventNum()) {
                    DistinctWinStateMetric metric = StatementStateMetric.createDistinctStateMetric(statementName);
                    this.timeWindow = new DistinctTimeWindow(agentInstanceContext, distinctByNode, FeatureToggle.getNumDistinctEventRetained(), metric);
                } else {
                    WinStateMetric metric = StatementStateMetric.createWinStateMetric(statementName);
                    this.timeWindow = new TimeWindow(agentInstanceViewFactoryContext.isRemoveStream(), metric);
                }
            }
        } else {
            this.timeWindow = new TimeWindow(agentInstanceViewFactoryContext.isRemoveStream());
        }
        this.timeWindow.setTimeDelta(this.timeDeltaComputation);
    }

    private ExprNode[] getGroupByNodes(AgentInstanceContext agentInstanceContext) {
        if (agentInstanceContext == null) {
            return null;
        }
        if (agentInstanceContext.getStatementSpec() == null) {
            return null;
        }
        GroupByClauseExpressions groupByClauseExpression = agentInstanceContext.getStatementSpec().getGroupByExpressions();
        if (groupByClauseExpression == null) {
            return null;
        }
        return groupByClauseExpression.getGroupByNodes();
    }

    private ExprNode getHavingDistinctNodes(AgentInstanceContext agentInstanceContext) {
        if (agentInstanceContext == null) {
            return null;
        }
        if (agentInstanceContext.getStatementSpec() == null) {
            return null;
        }
        ExprNode havingExprNode = agentInstanceContext.getStatementSpec().getHavingExprRootNode();
        ExprNode[] childNodes = havingExprNode.getChildNodes();
        for (ExprNode childNode : childNodes) {
            if (childNode instanceof ExprCountNode) {
                boolean isDistinct = ((ExprCountNode) childNode).isDistinct();
                if (isDistinct) {
                    ExprNode[] distinctByNodes = childNode.getChildNodes();
                    for (ExprNode exprNode : distinctByNodes) {
                        if (exprNode instanceof ExprIdentNode) {
                            return exprNode;
                        }
                    }
                }
            }
        }
        return null;
    }

    public View cloneView() {
        View view = externallyTimedWindowViewFactory.makeView(agentInstanceViewFactoryContext);

        // Group by 时可能会涉及多个view共用一个AgentInstanceContext
        if (this.agentInstanceViewFactoryContext.getAgentInstanceContext() != null) {
            if (view instanceof ExternallyTimedWindowView) {
                this.agentInstanceViewFactoryContext.getAgentInstanceContext().addExtTimedWindowView((ExternallyTimedWindowView) view);
            }
        }
        return view;
    }

    /**
     * Returns the field name to get timestamp values from.
     *
     * @return field name for timestamp values
     */
    public final ExprNode getTimestampExpression() {
        return timestampExpression;
    }

    public ExprTimePeriodEvalDeltaConst getTimeDeltaComputation() {
        return timeDeltaComputation;
    }

    public final EventType getEventType() {
        // The schema is the parent view's schema
        return parent.getEventType();
    }

    public final void update(EventBean[] newData, EventBean[] oldData) {
        if (InstrumentationHelper.ENABLED) {
            InstrumentationHelper.get().qViewProcessIRStream(this, externallyTimedWindowViewFactory.getViewName(), newData, oldData);
        }
        long timestamp = -1;

        // add data points to the window
        // we don't care about removed data from a prior view
        if (newData != null) {
            for (int i = 0; i < newData.length; i++) {
                timestamp = getLongValue(newData[i]);
                if (!timeWindow.add(timestamp, newData[i])) {
                    newData[i] = null;
                }
            }
        }

        // Remove from the window any events that have an older timestamp then the last event's timestamp
        ArrayDeque<EventBean> expired = null;
        if (timestamp != -1) {
            expired = timeWindow.expireEvents(timestamp - timeDeltaComputation.deltaSubtract(timestamp) + 1);
        }

        EventBean[] oldDataUpdate = null;
        if ((expired != null) && (!expired.isEmpty())) {
            oldDataUpdate = expired.toArray(new EventBean[expired.size()]);
        }

        if ((oldData != null) && (agentInstanceViewFactoryContext.isRemoveStream())) {
            for (EventBean anOldData : oldData) {
                timeWindow.remove(anOldData);
            }

            if (oldDataUpdate == null) {
                oldDataUpdate = oldData;
            } else {
                oldDataUpdate = CollectionUtil.addArrayWithSetSemantics(oldData, oldDataUpdate);
            }
        }

        if (viewUpdatedCollection != null) {
            viewUpdatedCollection.update(newData, oldDataUpdate);
        }

        // If there are child views, fireStatementStopped update method
        if (this.hasViews()) {
            if (InstrumentationHelper.ENABLED) {
                InstrumentationHelper.get().qViewIndicate(this, externallyTimedWindowViewFactory.getViewName(), newData, oldDataUpdate);
            }
            updateChildren(newData, oldDataUpdate);
            if (InstrumentationHelper.ENABLED) {
                InstrumentationHelper.get().aViewIndicate();
            }
        }

        if (InstrumentationHelper.ENABLED) {
            InstrumentationHelper.get().aViewProcessIRStream();
        }
    }

    public final Iterator<EventBean> iterator() {
        return timeWindow.iterator();
    }

    public final String toString() {
        return this.getClass().getName() +
                " timestampExpression=" + timestampExpression;
    }

    public void visitView(ViewDataVisitor viewDataVisitor) {
        timeWindow.visitView(viewDataVisitor, externallyTimedWindowViewFactory);
    }

    private long getLongValue(EventBean obj) {
        eventsPerStream[0] = obj;
        Number num = (Number) timestampExpressionEval.evaluate(eventsPerStream, true, agentInstanceViewFactoryContext);
        eventsPerStream[0] = null;//坑爹的泄露！！
        return num.longValue();
    }

    /**
     * Returns true to indicate the window is empty, or false if the view is not empty.
     *
     * @return true if empty
     */
    public boolean isEmpty() {
        return timeWindow.isEmpty();
    }

    public ViewUpdatedCollection getViewUpdatedCollection() {
        return viewUpdatedCollection;
    }

    public ViewFactory getViewFactory() {
        return externallyTimedWindowViewFactory;
    }

    public void applyClear(Object... groupByKeys) {
        // 在非GroupBy场景下，直接清空窗口内容
        // GroupBy场景下维持原逻辑
        if (isGroupedExtTimeWindow) {
            ((GroupWindow) this.timeWindow).clearAll(groupByKeys);
        } else {
            this.timeWindow.clearAll();
        }
    }
}
