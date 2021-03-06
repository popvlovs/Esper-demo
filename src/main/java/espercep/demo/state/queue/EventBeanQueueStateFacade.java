package espercep.demo.state.queue;

import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/3/18
 */
public class EventBeanQueueStateFacade {

    public static Set<Queue<? extends Map>> getQueueState(int ruleId, Map element) {
        return BootstrapBeanQueueState.singleton().getQueueState(ruleId, element);
    }

    public static void addQueueState(int ruleId, Map element, Queue<? extends Map> queue) {
        BootstrapBeanQueueState.singleton().addQueueState(ruleId, element, queue);
    }

    public static void removeQueueState(int ruleId, Map element, Queue<? extends Map> queue) {
        BootstrapBeanQueueState.singleton().removeQueueState(ruleId, element, queue);
    }

    public static void removeAllQueueState(int ruleId, Map element) {
        BootstrapBeanQueueState.singleton().removeAllQueueState(ruleId, element);
    }
}
