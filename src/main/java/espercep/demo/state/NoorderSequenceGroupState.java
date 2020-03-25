package espercep.demo.state;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/3/16
 */
public class NoorderSequenceGroupState<E extends Map> {
    private final Map<String, NoorderSequenceState<E>> groupState = new ConcurrentHashMap<>();
    private int numEventSlots;
    private int ruleId;

    public NoorderSequenceGroupState(int numEventSlots, int ruleId) {
        this.numEventSlots = numEventSlots;
        this.ruleId = ruleId;
    }

    public void offer(String groupKey, int slot, E element) {
        getStateByGroupKey(groupKey).offer(slot, element);
    }

    /*public E poll(String groupKey, int slot) {
        return getStateByGroupKey(groupKey).poll(slot);
    }*/

    private NoorderSequenceState<E> getStateByGroupKey(String groupKey) {
        if (!groupState.containsKey(groupKey)) {
            synchronized (groupState) {
                if (!groupState.containsKey(groupKey)) {
                    NoorderSequenceState<E> state = new NoorderSequenceState<>(this.numEventSlots, this.ruleId);
                    groupState.put(groupKey, state);
                    return state;
                }
            }
        }
        return groupState.get(groupKey);
    }

    public List<List<E>> pollCompleteSequence() {
        List<List<E>> completeSequence = new ArrayList<>();
        for (String groupKey : groupState.keySet()) {
            NoorderSequenceState<E> state = getStateByGroupKey(groupKey);
            List<E> groupedCompleteSequence = state.pollCompleteSequence();
            if (groupedCompleteSequence != null) {
                completeSequence.add(groupedCompleteSequence);
            }
        }
        return completeSequence;
    }
}
