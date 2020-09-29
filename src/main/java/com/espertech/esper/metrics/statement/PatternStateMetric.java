package com.espertech.esper.metrics.statement;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/9/8
 */
public class PatternStateMetric extends StatementStateMetric {
    private AtomicInteger subExpression = new AtomicInteger(0);

    public PatternStateMetric(String name) {
        super(name);
    }

    @Override
    protected PatternStateMetric clear() {
        subExpression.set(0);
        return this;
    }

    public int getSubExpression() {
        return subExpression.get();
    }

    public void setSubExpression(int subExpression) {
        this.subExpression.set(subExpression);
    }

    public void incSubExpression() {
        this.subExpression.incrementAndGet();
    }

    public void decSubExpression() {
        this.subExpression.decrementAndGet();
    }
}
