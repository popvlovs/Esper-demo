package com.espertech.esper.metrics.statement;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/09/06
 */
public class DistinctGroupWinStateMetric extends StatementStateMetric {
    private volatile int innerWinSize;
    private volatile int groupWinSize;
    private volatile int groupSize;

    public int getInnerWinSize() {
        return innerWinSize;
    }

    public void setInnerWinSize(int innerWinSize) {
        this.innerWinSize = innerWinSize;
    }

    public int getGroupWinSize() {
        return groupWinSize;
    }

    public void setGroupWinSize(int groupWinSize) {
        this.groupWinSize = groupWinSize;
    }

    public void incGroupWinSize() {
        this.groupWinSize++;
    }

    public void decGroupWinSize() {
        this.groupWinSize--;
    }

    public int getGroupSize() {
        return groupSize;
    }

    public void setGroupSize(int groupSize) {
        this.groupSize = groupSize;
    }
}
