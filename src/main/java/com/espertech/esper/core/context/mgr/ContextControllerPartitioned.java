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
package com.espertech.esper.core.context.mgr;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.context.*;
import com.espertech.esper.collection.MultiKeyUntyped;
import com.espertech.esper.core.context.util.ContextControllerSelectorUtil;
import com.espertech.esper.core.context.util.StatementAgentInstanceUtil;
import com.espertech.esper.epl.spec.ContextDetailPartitionItem;
import com.espertech.esper.event.EventAdapterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ContextControllerPartitioned implements ContextController, ContextControllerPartitionedInstanceCreateCallback {
    private static final Logger logger = LoggerFactory.getLogger("com.hansight.esper.CTX");
    protected final int pathId;
    protected final ContextControllerLifecycleCallback activationCallback;
    protected final ContextControllerPartitionedFactoryImpl factory;

    protected final List<ContextControllerPartitionedFilterCallback> filterCallbacks = new ArrayList<ContextControllerPartitionedFilterCallback>();
    protected final Map<Object, ContextControllerInstanceHandle> partitionKeys = new ConcurrentHashMap<>();

    //protected final Map<Object, TimeWindowPair> timeWindows = new ConcurrentHashMap<>();
    //protected final ArrayDeque<TimeWindowPair> orderedTimeWindowPairs = new ArrayDeque<>(2048);

    private ContextInternalFilterAddendum activationFilterAddendum;
    protected int currentSubpathId;

    public ContextControllerPartitioned(int pathId, ContextControllerLifecycleCallback activationCallback, ContextControllerPartitionedFactoryImpl factory) {
        this.pathId = pathId;
        this.activationCallback = activationCallback;
        this.factory = factory;

        //把自己注册给定时调度服务
        //ScheduleTask.registerScheduleTask(this);
    }

    public void importContextPartitions(ContextControllerState state, int pathIdToUse, ContextInternalFilterAddendum filterAddendum, AgentInstanceSelector agentInstanceSelector) {
        initializeFromState(null, null, filterAddendum, state, pathIdToUse, agentInstanceSelector, true);
    }

    public void deletePath(ContextPartitionIdentifier identifier) {
        ContextPartitionIdentifierPartitioned partitioned = (ContextPartitionIdentifierPartitioned) identifier;
        partitionKeys.remove(getKeyObjectForLookup(partitioned.getKeys()));
    }

    public void visitSelectedPartitions(ContextPartitionSelector contextPartitionSelector, ContextPartitionVisitor visitor) {
        int nestingLevel = factory.getFactoryContext().getNestingLevel();
        if (contextPartitionSelector instanceof ContextPartitionSelectorFiltered) {
            ContextPartitionSelectorFiltered filtered = (ContextPartitionSelectorFiltered) contextPartitionSelector;

            ContextPartitionIdentifierPartitioned identifier = new ContextPartitionIdentifierPartitioned();
            for (Map.Entry<Object, ContextControllerInstanceHandle> entry : partitionKeys.entrySet()) {
                identifier.setContextPartitionId(entry.getValue().getContextPartitionOrPathId());
                Object[] identifierOA = getKeyObjectsAccountForMultikey(entry.getKey());
                identifier.setKeys(identifierOA);

                if (filtered.filter(identifier)) {
                    visitor.visit(nestingLevel, pathId, factory.getBinding(), identifierOA, this, entry.getValue());
                }
            }
            return;
        } else if (contextPartitionSelector instanceof ContextPartitionSelectorSegmented) {
            ContextPartitionSelectorSegmented partitioned = (ContextPartitionSelectorSegmented) contextPartitionSelector;
            if (partitioned.getPartitionKeys() == null || partitioned.getPartitionKeys().isEmpty()) {
                return;
            }
            for (Object[] keyObjects : partitioned.getPartitionKeys()) {
                Object key = getKeyObjectForLookup(keyObjects);
                ContextControllerInstanceHandle instanceHandle = partitionKeys.get(key);
                if (instanceHandle != null && instanceHandle.getContextPartitionOrPathId() != null) {
                    visitor.visit(nestingLevel, pathId, factory.getBinding(), keyObjects, this, instanceHandle);
                }
            }
            return;
        } else if (contextPartitionSelector instanceof ContextPartitionSelectorById) {
            ContextPartitionSelectorById filtered = (ContextPartitionSelectorById) contextPartitionSelector;

            for (Map.Entry<Object, ContextControllerInstanceHandle> entry : partitionKeys.entrySet()) {
                if (filtered.getContextPartitionIds().contains(entry.getValue().getContextPartitionOrPathId())) {
                    visitor.visit(nestingLevel, pathId, factory.getBinding(), getKeyObjectsAccountForMultikey(entry.getKey()), this, entry.getValue());
                }
            }
            return;
        } else if (contextPartitionSelector instanceof ContextPartitionSelectorAll) {
            for (Map.Entry<Object, ContextControllerInstanceHandle> entry : partitionKeys.entrySet()) {
                visitor.visit(nestingLevel, pathId, factory.getBinding(), getKeyObjectsAccountForMultikey(entry.getKey()), this, entry.getValue());
            }
            return;
        }
        throw ContextControllerSelectorUtil.getInvalidSelector(new Class[]{ContextPartitionSelectorSegmented.class}, contextPartitionSelector);
    }

    public void activate(EventBean optionalTriggeringEvent, Map<String, Object> optionalTriggeringPattern, ContextControllerState controllerState, ContextInternalFilterAddendum filterAddendum, Integer importPathId) {
        ContextControllerFactoryContext factoryContext = factory.getFactoryContext();
        this.activationFilterAddendum = filterAddendum;
        for (ContextDetailPartitionItem item : factory.getSegmentedSpec().getItems()) {
            ContextControllerPartitionedFilterCallback callback = new ContextControllerPartitionedFilterCallback(factoryContext.getServicesContext(), factoryContext.getAgentInstanceContextCreate(), item, this, filterAddendum);
            filterCallbacks.add(callback);

            if (optionalTriggeringEvent != null) {
                boolean match = StatementAgentInstanceUtil.evaluateFilterForStatement(factoryContext.getServicesContext(), optionalTriggeringEvent, factoryContext.getAgentInstanceContextCreate(), callback.getFilterHandle());

                if (match) {
                    callback.matchFound(optionalTriggeringEvent, null);
                }
            }
        }

        if (factoryContext.getNestingLevel() == 1) {
            controllerState = ContextControllerStateUtil.getRecoveryStates(factory.getFactoryContext().getStateCache(), factoryContext.getOutermostContextName());
        }

        /*if(activationCallback instanceof ContextManagerImpl){
            for(Map.Entry<Integer, ContextControllerStatementDesc> entry : ((ContextManagerImpl)activationCallback).getStatements().entrySet()){
                ContextControllerStatementDesc ccsd = entry.getValue();
                Pattern pattern = Pattern.compile(", (\\d{1,5} \\w{3,5})\\)");
                Matcher matcher = pattern.matcher(ccsd.getStatement().getStatementContext().getEpStatementHandle().getEPL());
                if(matcher.find()){
                    String timeWindow = matcher.group(1);
                    String[] time_unit = timeWindow.split(" ");
                    if(time_unit.length == 2){
                        switch (time_unit[1]){
                            case "sec":
                            case "second":
                                maxRetentionTime = Integer.parseInt(time_unit[0]) * 1000;
                                break;
                            case "min":
                            case "minute":
                                maxRetentionTime = Integer.parseInt(time_unit[0]) * 60* 1000;
                                break;
                            case "hour":
                                maxRetentionTime = Integer.parseInt(time_unit[0]) * 60 * 60 *1000;
                                break;
                            case "day":
                                maxRetentionTime = Integer.parseInt(time_unit[0]) * 24 * 60 * 60 *1000;
                                break;
                            case "week":
                                maxRetentionTime = Integer.parseInt(time_unit[0]) * 7 * 24 * 60 * 60 *1000;
                                break;
                            default:
                                maxRetentionTime = 10 * 60 * 1000;//默认保持10min
                        }
                    }
                }
            }
        }
        logger.info("CTX[{}]窗口数据最长保持时间：{}ms", factory.getFactoryContext().getContextName(), maxRetentionTime);*/
        if (controllerState == null) {
            return;
        }

        int pathIdToUse = importPathId != null ? importPathId : pathId;
        initializeFromState(optionalTriggeringEvent, optionalTriggeringPattern, filterAddendum, controllerState, pathIdToUse, null, false);
    }

    public ContextControllerFactory getFactory() {
        return factory;
    }

    public int getPathId() {
        return pathId;
    }

    public synchronized void deactivate() {
        ContextControllerFactoryContext factoryContext = factory.getFactoryContext();
        for (ContextControllerPartitionedFilterCallback callback : filterCallbacks) {
            callback.destroy(factoryContext.getServicesContext().getFilterService());
        }
        partitionKeys.clear();
        filterCallbacks.clear();

        /*timeWindows.clear();
        orderedTimeWindowPairs.clear();*/

        factory.getFactoryContext().getStateCache().removeContextParentPath(factoryContext.getOutermostContextName(), factoryContext.getNestingLevel(), pathId);
    }
    private int processDelt = 0;
    public synchronized void create(Object key, EventBean theEvent) {
        boolean exists = partitionKeys.containsKey(key);
        if (exists) {
            /*long newestTime = timeWindows.get(key).views.getLast().getTimestampValue(theEvent);
            if(orderedTimeWindowPairs.getLast().lastTimestamp > newestTime){
                //说明这是一个乱序数据，不做处理
                return;
            }
            //更新窗口最新时间
            timeWindows.get(key).lastTimestamp = newestTime;
            if(processDelt++ % 1000 == 0) {
                long startCacl = System.currentTimeMillis();
                for (TimeWindowPair twp : orderedTimeWindowPairs) {
                    twp.expireData(timeWindows.get(key).lastTimestamp);
                }
                processDelt = 0;
                if(System.currentTimeMillis() - startCacl > 0)
                    logger.debug("{} 遍历过期策略耗时：{}", factory.getFactoryContext().getContextName(), (System.currentTimeMillis() - startCacl));
            }*/
            return;
        }

        currentSubpathId++;

        // determine properties available for querying
        ContextControllerFactoryContext factoryContext = factory.getFactoryContext();
        Map<String, Object> props = ContextPropertyEventType.getPartitionBean(factoryContext.getContextName(), 0, key, factory.getSegmentedSpec().getItems().get(0).getPropertyNames());

        // merge filter addendum, if any
        ContextInternalFilterAddendum filterAddendum = activationFilterAddendum;
        if (factory.hasFiltersSpecsNestedContexts()) {
            filterAddendum = activationFilterAddendum != null ? activationFilterAddendum.deepCopy() : new ContextInternalFilterAddendum();
            factory.populateContextInternalFilterAddendums(filterAddendum, key);
        }

        ContextControllerInstanceHandle handle = activationCallback.contextPartitionInstantiate(null, currentSubpathId, null, this, theEvent, null, key, props, null, filterAddendum, false, ContextPartitionState.STARTED);

        partitionKeys.put(key, handle);

        /*Iterator<AgentInstance> iterator = handle.getInstances().getAgentInstances().iterator();
        //一个TWP代表了一个分区下，所有相关规则的时间窗口
        TimeWindowPair timeWindowPair = new TimeWindowPair();
        timeWindowPair.partitionKey = key;
        timeWindows.put(key, timeWindowPair);
        while (iterator.hasNext()){
            AgentInstance agentInstance = iterator.next();
            if(agentInstance.getFinalView() instanceof OutputProcessViewDirect){
                Viewable view = ((OutputProcessViewDirect)agentInstance.getFinalView()).getParent();
                if(view instanceof HackedView){
                    if(timeWindowPair.lastTimestamp <= 0) {
                        timeWindowPair.lastTimestamp = ((HackedView) view).getTimestampValue(theEvent);
                    }
                    timeWindowPair.addLast( (HackedView)view);
                }
            }
        }
        for(TimeWindowPair twp : orderedTimeWindowPairs){
            twp.expireData(timeWindowPair.lastTimestamp);
        }
        orderedTimeWindowPairs.addLast(timeWindowPair);*/


        // update the filter version for this handle
        long filterVersion = factoryContext.getServicesContext().getFilterService().getFiltersVersion();
        factory.getFactoryContext().getAgentInstanceContextCreate().getEpStatementAgentInstanceHandle().getStatementFilterVersion().setStmtFilterVersion(filterVersion);

        Object[] keyObjectSaved = getKeyObjectsAccountForMultikey(key);
        factory.getFactoryContext().getStateCache().addContextPath(factoryContext.getOutermostContextName(), factoryContext.getNestingLevel(), pathId, currentSubpathId, handle.getContextPartitionOrPathId(), keyObjectSaved, factory.getBinding());
    }

    private Object[] getKeyObjectsAccountForMultikey(Object key) {
        if (key instanceof MultiKeyUntyped) {
            return ((MultiKeyUntyped) key).getKeys();
        } else {
            return new Object[]{key};
        }
    }

    private Object getKeyObjectForLookup(Object[] keyObjects) {
        if (keyObjects.length > 1) {
            return new MultiKeyUntyped(keyObjects);
        } else {
            return keyObjects[0];
        }
    }

    private void initializeFromState(EventBean optionalTriggeringEvent,
                                     Map<String, Object> optionalTriggeringPattern,
                                     ContextInternalFilterAddendum filterAddendum,
                                     ContextControllerState controllerState,
                                     int pathIdToUse,
                                     AgentInstanceSelector agentInstanceSelector,
                                     boolean loadingExistingState) {

        ContextControllerFactoryContext factoryContext = factory.getFactoryContext();
        TreeMap<ContextStatePathKey, ContextStatePathValue> states = controllerState.getStates();

        // restart if there are states
        int maxSubpathId = Integer.MIN_VALUE;
        NavigableMap<ContextStatePathKey, ContextStatePathValue> childContexts = ContextControllerStateUtil.getChildContexts(factoryContext, pathIdToUse, states);
        EventAdapterService eventAdapterService = factory.getFactoryContext().getServicesContext().getEventAdapterService();

        for (Map.Entry<ContextStatePathKey, ContextStatePathValue> entry : childContexts.entrySet()) {
            Object[] keys = (Object[]) factory.getBinding().byteArrayToObject(entry.getValue().getBlob(), eventAdapterService);
            Object mapKey = getKeyObjectForLookup(keys);

            // merge filter addendum, if any
            ContextInternalFilterAddendum myFilterAddendum = activationFilterAddendum;
            if (factory.hasFiltersSpecsNestedContexts()) {
                filterAddendum = activationFilterAddendum != null ? activationFilterAddendum.deepCopy() : new ContextInternalFilterAddendum();
                factory.populateContextInternalFilterAddendums(filterAddendum, mapKey);
            }

            // check if exists already
            if (controllerState.isImported()) {
                ContextControllerInstanceHandle existingHandle = partitionKeys.get(mapKey);
                if (existingHandle != null) {
                    activationCallback.contextPartitionNavigate(existingHandle, this, controllerState, entry.getValue().getOptionalContextPartitionId(), myFilterAddendum, agentInstanceSelector, entry.getValue().getBlob(), loadingExistingState);
                    continue;
                }
            }

            Map<String, Object> props = ContextPropertyEventType.getPartitionBean(factoryContext.getContextName(), 0, mapKey, factory.getSegmentedSpec().getItems().get(0).getPropertyNames());

            int assignedSubpathId = !controllerState.isImported() ? entry.getKey().getSubPath() : ++currentSubpathId;
            ContextControllerInstanceHandle handle = activationCallback.contextPartitionInstantiate(entry.getValue().getOptionalContextPartitionId(), assignedSubpathId, entry.getKey().getSubPath(), this, optionalTriggeringEvent, optionalTriggeringPattern, mapKey, props, controllerState, myFilterAddendum, loadingExistingState || factoryContext.isRecoveringResilient(), entry.getValue().getState());
            partitionKeys.put(mapKey, handle);

            if (entry.getKey().getSubPath() > maxSubpathId) {
                maxSubpathId = assignedSubpathId;
            }
        }
        if (!controllerState.isImported()) {
            currentSubpathId = maxSubpathId != Integer.MIN_VALUE ? maxSubpathId : 0;
        }
    }
}