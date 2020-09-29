package com.espertech.esper.metrics.statement;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/09/06
 */
public class DistinctWinStateMetric extends StatementStateMetric {
    private volatile int innerWinSize;
    private volatile int distinctWinSize;

    public DistinctWinStateMetric(String name) {
        super(name);
    }

    @Override
    protected DistinctWinStateMetric clear() {
        this.innerWinSize = 0;
        this.distinctWinSize = 0;
        return this;
    }

    public int getInnerWinSize() {
        return innerWinSize;
    }

    public void setInnerWinSize(int innerWinSize) {
        this.innerWinSize = innerWinSize;
    }

    public int getDistinctWinSize() {
        return distinctWinSize;
    }

    public void setDistinctWinSize(int distinctWinSize) {
        this.distinctWinSize = distinctWinSize;
    }

    public void incDistinctWinSize() {
        this.distinctWinSize++;
    }

    public void decDistinctWinSize() {
        this.distinctWinSize--;
    }
}
