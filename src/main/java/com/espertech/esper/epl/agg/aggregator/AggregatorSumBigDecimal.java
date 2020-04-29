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

import java.math.BigDecimal;

/**
 * Sum for BigInteger values.
 */
public class AggregatorSumBigDecimal implements AggregationMethod {
    protected BigDecimal sum;
    protected long numDataPoints;
    protected long clearTag;
    /**
     * Ctor.
     */
    public AggregatorSumBigDecimal() {
        sum = new BigDecimal(0.0);
    }

    public void clear() {
        clearTag += numDataPoints;
        sum = new BigDecimal(0.0);
        numDataPoints = 0;
    }

    public void enter(Object object) {
        if (object == null) {
            return;
        }
        numDataPoints++;
        sum = sum.add((BigDecimal) object);
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
                sum = sum.subtract((BigDecimal) object);
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
