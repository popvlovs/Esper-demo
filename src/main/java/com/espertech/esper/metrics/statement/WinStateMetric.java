package com.espertech.esper.metrics.statement;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/09/06
 */
public class WinStateMetric extends StatementStateMetric {
    private volatile int innerWinSize;

    public WinStateMetric(String name) {
        super(name);
    }

    @Override
    protected WinStateMetric clear() {
        this.innerWinSize = 0;
        return this;
    }

    public int getInnerWinSize() {
        return innerWinSize;
    }

    public void setInnerWinSize(int innerWinSize) {
        this.innerWinSize = innerWinSize;
    }
}
