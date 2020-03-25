package espercep.demo.state.queue;

import java.util.*;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * 用于记录每个event bean在哪些queue中存在，便于expire时从这个queue中快速移除event bean
 * 将queue信息直接存储在event bean中，问题是破坏了event bean的数据结构，在输出的时候需要移除这些字段
 * 并且event bean默认不是线程安全的Map，因此在多线程场景下需要保证线程安全
 *
 * @author yitian_song 2020/3/18
 */
public class BootstrapBeanQueueState implements IEventBeanQueueState {
    private final static BootstrapBeanQueueState singleton = new BootstrapBeanQueueState();
    private final static String QUEUE_STATE_KEY_PREFIX = "_queue_state_";

    private BootstrapBeanQueueState() {
    }

    public static BootstrapBeanQueueState singleton() {
        return singleton;
    }

    @SuppressWarnings("unchecked")
    public Set<Queue<? extends Map>> getQueueState(int ruleId, Map element) {
        String key = QUEUE_STATE_KEY_PREFIX + ruleId;
        if (!element.containsKey(key)) {
            synchronized (element) {
                if (!element.containsKey(key)) {
                    element.put(key, Collections.synchronizedSet(new HashSet<>()));
                }
            }
        }
        return (Set<Queue<? extends Map>>) element.get(key);
    }

    @SuppressWarnings("unchecked")
    public void addQueueState(int ruleId, Map element, Queue<? extends Map> queue) {
        getQueueState(ruleId, element).add(queue);
    }

    public void removeQueueState(int ruleId, Map element, Queue<? extends Map> queue) {
        getQueueState(ruleId, element).remove(queue);
    }

    public void removeAllQueueState(int ruleId, Map element) {
        String key = QUEUE_STATE_KEY_PREFIX + ruleId;
        element.remove(key);
    }
}
