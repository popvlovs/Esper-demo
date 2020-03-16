package espercep.demo.udf;

import espercep.demo.state.NoorderSequenceGroupState;
import espercep.demo.state.NoorderSequenceState;
import espercep.demo.state.NotBeforeMapState;
import espercep.demo.util.MetricUtil;

import java.util.*;
import java.util.concurrent.*;

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

    private final static Map<String, NoorderSequenceGroupState<Map>> noorderSequenceGroupState = new ConcurrentHashMap<>();

    private static NoorderSequenceGroupState<Map> getNoorderSequenceGroupState(String ruleId, int totalTagChannel) {
        if (!noorderSequenceGroupState.containsKey(ruleId)) {
            synchronized (noorderSequenceGroupState) {
                if (!noorderSequenceGroupState.containsKey(ruleId)) {
                    noorderSequenceGroupState.put(ruleId, new NoorderSequenceGroupState<>(totalTagChannel));
                }
            }
        }
        return noorderSequenceGroupState.get(ruleId);
    }

    /**
     * 事件进入Esper时间窗口后执行，将事件同步载入自定义数据结构中，进行无序事件检测
     * @param ruleId            规则ID
     * @param tagChannel        事件分类索引
     * @param totalTagChannel   事件总分类数
     * @param groupKey          事件分组key
     * @param eventBean         事件对象
     */
    public static boolean onCase(String ruleId, int tagChannel, int totalTagChannel, Object groupKey, Map eventBean) {
        NoorderSequenceGroupState<Map> state = getNoorderSequenceGroupState(ruleId, totalTagChannel);
        state.offer(groupKey.toString(), tagChannel, eventBean);
        Map<String, List<List<Map>>> completeEvents = UserDefinedFunction.findCompleteEvents();
        if (!completeEvents.isEmpty()) {
            completeEvents.forEach((rule, sequence) -> {
                long error = sequence.stream().filter(list -> list.stream().anyMatch(Objects::isNull)).count();
                MetricUtil.getCounter("Complete sequence error of " + ruleId).inc(error);
                MetricUtil.getCounter("Complete sequence of " + ruleId).inc(sequence.size());
            });
        }
        return true;
    }

    /**
     * 在事件移出esper时间窗口后，将该事件从内存结构中同步移除
     * onEventExpired 和 findCompleteEvents都在listener线程中
     * listener线程默认只有一个，因此不用考虑在这两个方法之间加锁
     * 如果上述假设不成立，那么由于两个方法都按条件执行了queue.poll()
     * 为了保证原子性，必须要加锁，否则可能导致queue.poll被重复执行
     */
    @SuppressWarnings("unchecked")
    public static void onEventExpired(Map eventBean) {
        if (eventBean.containsKey("state_refs")) {
            Set<Queue> belongedQueue = (Set<Queue>) eventBean.get("state_refs");
            belongedQueue.forEach(queue -> {
                NoorderSequenceState.LinkedQueueWrapper queueWrapper = (NoorderSequenceState.LinkedQueueWrapper) queue;
                Map element = queueWrapper.pollAsParentState();
                if (!Objects.equals(element.get("id"), eventBean.get("id"))) {
                    MetricUtil.getCounter("Unexpected poll").inc();
                }
            });
            eventBean.remove("state_refs");
        }
    }

    /**
     * 获取每个无序匹配规则中，已经完成匹配的事件序列
     * 与
     */
    public static Map<String, List<List<Map>>> findCompleteEvents() {
        Map<String, List<List<Map>>> completeEvents = new HashMap<>();
        noorderSequenceGroupState.forEach((ruleId, groupState) -> {
            List<List<Map>> events = groupState.pollCompleteSequence();
            if (!events.isEmpty()) {
                completeEvents.put(ruleId, events);
            }
        });
        return completeEvents;
    }
}
