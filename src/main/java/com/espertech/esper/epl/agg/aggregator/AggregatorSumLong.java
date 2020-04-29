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
package com.espertech.esper.epl.agg.aggregator;

/**
 * Sum for long values.
 */
public class AggregatorSumLong implements AggregationMethod {
    protected long sum;
    protected long numDataPoints;
    protected long clearTag;
    public void clear() {
        clearTag += numDataPoints;
        sum = 0;
        numDataPoints = 0;
    }

    public void enter(Object object) {
        if (object == null) {
            return;
        }
        numDataPoints++;
        sum += (Long) object;
    }

    public void leave(Object object) {
        if (object == null) {
            return;
        }

        if(clearTag > 0){
            clearTag--;
        }
        else {
            if (numDataPoints <= 1) {
                clear();
            } else {
                numDataPoints--;
                sum -= (Long) object;
            }
        }
    }

    public Object getValue() {
        if (numDataPoints == 0) {
            return null;
        }
        return sum;
    }

}


