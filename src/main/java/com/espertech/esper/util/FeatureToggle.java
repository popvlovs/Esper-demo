package com.espertech.esper.util;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/4/27
 */
public class FeatureToggle {
    /**
     * 是否开启：当having触发时，将对应数据从window中移除，以优化内存使用
     */
    private static boolean discardExtTimedWindowOnAggOutput;

    private static int numDistinctEventRetained;

    public static boolean isDiscardExtTimedWindowOnAggOutput() {
        return discardExtTimedWindowOnAggOutput;
    }

    public static void setDiscardExtTimedWindowOnAggOutput(boolean discardExtTimedWindowOnAggOutput) {
        FeatureToggle.discardExtTimedWindowOnAggOutput = discardExtTimedWindowOnAggOutput;
    }

    public static void setNumDistinctEventRetained(int numDistinctEventRetained) {
        FeatureToggle.numDistinctEventRetained = numDistinctEventRetained;
    }

    public static int getNumDistinctEventRetained() {
        return numDistinctEventRetained;
    }

    public static boolean isReduceDistinctEventNum() {
        return numDistinctEventRetained > 0;
    }
}
