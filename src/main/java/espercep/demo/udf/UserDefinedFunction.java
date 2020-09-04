package espercep.demo.udf;

import com.espertech.esper.client.hook.EPLMethodInvocationContext;
import espercep.demo.state.NoorderSequenceGroupState;
import espercep.demo.state.NoorderSequenceState;
import espercep.demo.state.NotBeforeMapState;
import espercep.demo.state.queue.EventBeanQueueStateFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/12/23
 * @description .
 */
public class UserDefinedFunction {
    private static final Logger logger = LoggerFactory.getLogger(UserDefinedFunction.class);

    private static final Map<String, NotBeforeMapState> notBeforeRuleStates = new ConcurrentHashMap<>();

    /**
     * Update last seen event-A to state
     *
     * @param ruleId  not-before rule id
     * @param event   event map
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
     * @param ruleId  not-before rule id
     * @param event   event map
     * @param within  within time window
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

    public static int winCount(Map[] maps, EPLMethodInvocationContext context) {
        return maps.length;
    }

    public static long firstOccurTime(Map[] maps) {
        return (long) maps[0].get("occur_time");
    }

    public static long lastOccurTime(Map[] maps) {
        return (long) maps[maps.length - 1].get("occur_time");
    }

    private final static AtomicLong lastOutputTsA = new AtomicLong(0L);
    private final static AtomicLong lastOutputTsB = new AtomicLong(0L);

    public static boolean timerTerminate(Map data, EPLMethodInvocationContext context) {
        long now = System.currentTimeMillis();
        if (Objects.equals(data.get("event_name"), "A")) {
            if (now - lastOutputTsA.get() > TimeUnit.SECONDS.toMillis(3)) {
                lastOutputTsA.set(now);
                return true;
            } else {
                return false;
            }
        } else {
            if (now - lastOutputTsB.get() > TimeUnit.SECONDS.toMillis(3)) {
                lastOutputTsB.set(now);
                return true;
            } else {
                return false;
            }
        }
    }

    private final static Map<Integer, NoorderSequenceGroupState<Map<String, Object>>> noorderSequenceGroupState = new ConcurrentHashMap<>();

    /**
     * 事件进入Esper时间窗口后执行，将事件同步载入自定义数据结构中，进行无序事件检测
     * Noorder sequence rule中的filter必须互斥，否则检出的序列中可能存在重复事件（个人认为不是个issue）
     *
     * @param ruleId    规则ID
     * @param slot      事件分类索引
     * @param numSlots  事件总分类数
     * @param groupKeys 事件分组key
     * @param eventBean 事件对象
     */
    public static boolean onCase(int ruleId, int slot, int numSlots, Map<String, Object> eventBean, Object... groupKeys) {
        NoorderSequenceGroupState<Map<String, Object>> state = getNoorderSequenceGroupState(ruleId, numSlots);
        StringBuilder groupBy = new StringBuilder();
        for (int i = 0; i < groupKeys.length; i++) {
            groupBy.append(Optional.ofNullable(groupKeys[i]).orElse("NULL"));
        }
        state.offer(groupBy.toString(), slot, eventBean);
        return true;
    }

    /**
     * 在事件移出esper时间窗口后，将该事件从内存结构中同步移除
     * 这个方法不是严格线程安全的，但是所有针对共享内存的不安全的操作主要集中在findCompleteEvents方法中
     * 而onEventExpired 和 findCompleteEvents都在listener线程中
     * listener线程默认只有一个，因此暂时不用考虑在这两个方法之间加锁
     * 如果将来要采用本地多线程跑多个EsperEngine的模式，那么这些EsperEngine都是共享同一个NoorderSequenceFunc & state
     * 因此到时候需要将所有的NoorderSequence rule放到同一个线程中去处理，避免多线程不安全问题
     */
    @SuppressWarnings("unchecked")
    public static void onEventExpired(int ruleId, Map eventBean) {
        try {
            Set<Queue<? extends Map>> belongedQueue = EventBeanQueueStateFacade.getQueueState(ruleId, eventBean);
            Iterator<Queue<? extends Map>> queueIterator = belongedQueue.iterator();
            while (queueIterator.hasNext()) {
                Queue<? extends Map> queue = queueIterator.next();
                NoorderSequenceState.LinkedQueueWrapper queueWrapper = (NoorderSequenceState.LinkedQueueWrapper) queue;
                Map polledElement = queueWrapper.pollAsParentState();
                if (!Objects.equals(polledElement, eventBean)) {
                    // Expect to poll someone but actually polled another one
                    //  in single-thread it should not happen
                    //  if happened, there must be something wrong
                    logger.error("Error on event expire, expect to poll queue [{}] but actually polled [{}]", eventBean, polledElement);
                }
            }
        } catch (Exception e) {
            // eventBean is not thread-safe(HashMap), there is some potential multi-thread problems
            // such as multi-thread read like eventBean.get(...) and belongedQueue.forEach(...)
            logger.error("Error on event expire: ", e);
        } finally {
            EventBeanQueueStateFacade.removeAllQueueState(ruleId, eventBean);
        }
    }

    /**
     * 获取每个无序匹配规则中，已经完成匹配的事件序列
     */
    public static Map<Integer, List<List<Map<String, Object>>>> findCompleteEvents() {
        Map<Integer, List<List<Map<String, Object>>>> completeEvents = new HashMap<>();
        noorderSequenceGroupState.forEach((ruleId, groupState) -> {
            List<List<Map<String, Object>>> events = groupState.pollCompleteSequence();
            if (!events.isEmpty()) {
                completeEvents.put(ruleId, events);
            }
        });
        return completeEvents;
    }

    /**
     * 获取指定无序匹配规则中，已经完成匹配的事件序列
     */
    public static Map<String, List<List<Map<String, Object>>>> findCompleteEvents(int ruleId) {
        NoorderSequenceGroupState<Map<String, Object>> state = noorderSequenceGroupState.get(ruleId);
        if (state == null) {
            return Collections.emptyMap();
        }
        List<List<Map<String, Object>>> events = state.pollCompleteSequence();
        if (!events.isEmpty()) {
            return Collections.singletonMap(Integer.toString(ruleId), events);
        } else {
            return Collections.emptyMap();
        }
    }

    private static NoorderSequenceGroupState<Map<String, Object>> getNoorderSequenceGroupState(int ruleId, int numSlots) {
        if (!noorderSequenceGroupState.containsKey(ruleId)) {
            synchronized (noorderSequenceGroupState) {
                if (!noorderSequenceGroupState.containsKey(ruleId)) {
                    noorderSequenceGroupState.put(ruleId, new NoorderSequenceGroupState<>(numSlots, ruleId));
                }
            }
        }
        return noorderSequenceGroupState.get(ruleId);
    }

    public static Map<String, Object> countWindow(String eventId, long occurTime, EPLMethodInvocationContext context, Object... groupByKeys) {
        return Collections.emptyMap();
    }

    public static boolean belongs(String argA, String argB) {
        return true;
    }

    public static boolean contains(String argA, String argB) {
        return true;
    }
}
