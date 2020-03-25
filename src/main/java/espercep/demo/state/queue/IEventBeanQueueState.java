package espercep.demo.state.queue;

import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/3/18
 */
public interface IEventBeanQueueState {
    Set<Queue<? extends Map>> getQueueState(int ruleId, Map element);

    void addQueueState(int ruleId, Map element, Queue<? extends Map> queue);

    void removeQueueState(int ruleId, Map element, Queue<? extends Map> queue);

    void removeAllQueueState(int ruleId, Map element);
}
