package espercep.demo.udf;

import espercep.demo.state.NotBeforeMapState;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/12/23
 * @description .
 */
public class UserDefinedFunction {
    private static final Map<String, NotBeforeMapState> notBeforeRuleStates = new ConcurrentHashMap<>();


    /**
     * Update last seen event-A to state
     *
     * @param ruleId not-before rule id
     * @param event event map
     * @param groupBy group by fields
     */
    public static boolean updateLastA(String ruleId, Map event, String... groupBy) {
        NotBeforeMapState notBeforeMapState = getNotBeforeState(ruleId);
        Map lastA = notBeforeMapState.getState(event, groupBy);
        long eventTime = getEventTime(event);
        if (lastA == null || eventTime >= getEventTime(lastA)) {
            notBeforeMapState.setState(event, groupBy);
        }
        return false;
    }


    /**
     * Check if event-A occurred within {time-window} before event-B
     *
     * @param ruleId not-before rule id
     * @param event event map
     * @param within within time window
     * @param groupBy group by fields
     */
    public static boolean notBefore(String ruleId, Map event, long within, String... groupBy) {
        NotBeforeMapState notBeforeMapState = getNotBeforeState(ruleId);
        Map lastA = notBeforeMapState.getState(event, groupBy);
        long eventTime = getEventTime(event);
        return (lastA == null && eventTime >= notBeforeMapState.getActiveTime() + within)
                || (lastA != null && eventTime >= (getEventTime(lastA) + within));
    }

    public static boolean setActiveTime(String ruleId, Map event) {
        NotBeforeMapState notBeforeMapState = getNotBeforeState(ruleId);
        long eventTime = getEventTime(event);
        notBeforeMapState.setMinActiveTime(eventTime);
        return true;
    }

    private static NotBeforeMapState getNotBeforeState(String ruleId) {
        if (!notBeforeRuleStates.containsKey(ruleId)) {
            synchronized (notBeforeRuleStates) {
                if (!notBeforeRuleStates.containsKey(ruleId)) {
                    notBeforeRuleStates.put(ruleId, new NotBeforeMapState());
                }
            }
        }
        return notBeforeRuleStates.get(ruleId);
    }

    private static long getEventTime(Map event) {
        Object eventTime = event.get("occur_time");
        if (eventTime == null) {
            return 0L;
        }
        if (eventTime instanceof String) {
            return Long.parseLong(eventTime.toString());
        } else {
            return (Long) eventTime;
        }
    }
}
