package com.hansight.hes.engine.ext.noorder.state.queue;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * 用于记录每个event bean在哪些queue中存在，便于expire时从这个queue中快速移除event bean
 * 用一个HashMap去存储，问题在于每次都要去搜索，会消耗一部分性能
 *
 * @author yitian_song 2020/3/18
 */
public class MapEventBeanQueueState implements IEventBeanQueueState {
    private final static MapEventBeanQueueState singleton = new MapEventBeanQueueState();

    private final Map<Object, Map<Object, Set<Queue<? extends Map>>>> state = new ConcurrentHashMap<>();

    private MapEventBeanQueueState() {
    }

    public static MapEventBeanQueueState singleton() {
        return singleton;
    }

    public Map<Object, Set<Queue<? extends Map>>> getEventBeanQueueState(Object ruleId) {
        if (!state.containsKey(ruleId)) {
            synchronized (state) {
                if (!state.containsKey(ruleId)) {
                    state.put(ruleId, new ConcurrentHashMap<>());
                }
            }
        }
        return state.get(ruleId);
    }

    public Set<Queue<? extends Map>> getQueueState(int ruleId, Map element) {
        Map<Object, Set<Queue<? extends Map>>> state = getEventBeanQueueState(ruleId);
        if (!state.containsKey(element)) {
            synchronized (state) {
                if (!state.containsKey(element)) {
                    state.put(element, Collections.synchronizedSet(new HashSet<>()));
                }
            }
        }
        return state.get(element);
    }

    @SuppressWarnings("unchecked")
    public void addQueueState(int ruleId, Map element, Queue<? extends Map> queue) {
        getQueueState(ruleId, element).add(queue);
    }

    public void removeQueueState(int ruleId, Map element, Queue<? extends Map> queue) {
        getQueueState(ruleId, element).remove(queue);
    }

    public void removeAllQueueState(int ruleId, Map element) {
        getEventBeanQueueState(ruleId).remove(element);
    }
}
