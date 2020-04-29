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
package com.espertech.esper.epl.join.pollindex;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventType;
import com.espertech.esper.core.service.StatementContext;
import com.espertech.esper.epl.join.table.EventTable;
import com.espertech.esper.epl.join.table.EventTableFactoryTableIdentStmt;
import com.espertech.esper.epl.join.table.PropertyCompositeEventTableFactory;
import com.espertech.esper.epl.join.table.UnindexedEventTableList;

import java.util.Arrays;
import java.util.List;

/**
 * Strategy for building an index out of poll-results knowing the properties to base the index on.
 */
public class PollResultIndexingStrategyComposite implements PollResultIndexingStrategy {
    private final int streamNum;
    private final EventType eventType;
    private final String[] indexPropertiesJoin;
    private final Class[] keyCoercionTypes;
    private final String[] rangePropertiesJoin;
    private final Class[] rangeCoercionTypes;

    public PollResultIndexingStrategyComposite(int streamNum, EventType eventType, String[] indexPropertiesJoin, Class[] keyCoercionTypes, String[] rangePropertiesJoin, Class[] rangeCoercionTypes) {
        this.streamNum = streamNum;
        this.eventType = eventType;
        this.keyCoercionTypes = keyCoercionTypes;
        this.indexPropertiesJoin = indexPropertiesJoin;
        this.rangePropertiesJoin = rangePropertiesJoin;
        this.rangeCoercionTypes = rangeCoercionTypes;
    }

    public EventTable[] index(List<EventBean> pollResult, boolean isActiveCache, StatementContext statementContext) {
        if (!isActiveCache) {
            return new EventTable[]{new UnindexedEventTableList(pollResult, streamNum)};
        }
        PropertyCompositeEventTableFactory factory = new PropertyCompositeEventTableFactory(streamNum, eventType, indexPropertiesJoin, keyCoercionTypes, rangePropertiesJoin, rangeCoercionTypes);
        EventTable[] tables = factory.makeEventTables(new EventTableFactoryTableIdentStmt(statementContext));
        for (EventTable table : tables) {
            table.add(pollResult.toArray(new EventBean[pollResult.size()]));
        }
        return tables;
    }

    public String toQueryPlan() {
        return this.getClass().getSimpleName() +
                " hash " + Arrays.toString(indexPropertiesJoin) +
                " btree " + Arrays.toString(rangePropertiesJoin) +
                " key coercion " + Arrays.toString(keyCoercionTypes) +
                " range coercion " + Arrays.toString(rangeCoercionTypes);
    }
}
