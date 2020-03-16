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
    private int numTagChannel;

    public NoorderSequenceGroupState(int numTagChannel) {
        this.numTagChannel = numTagChannel;
    }

    public void offer(String groupKey, int tagChannel, E element) {
        getStateByGroupKey(groupKey).offer(tagChannel, element);
    }

    private NoorderSequenceState<E> getStateByGroupKey(String groupKey) {
        if (!groupState.containsKey(groupKey)) {
            synchronized (groupState) {
                if (!groupState.containsKey(groupKey)) {
                    NoorderSequenceState<E> state = new NoorderSequenceState<>(this.numTagChannel);
                    groupState.put(groupKey, state);
                    return state;
                }
            }
        }
        return groupState.get(groupKey);
    }

    /*public E poll(String groupKey, int tagChannel) {
        return getStateByGroupKey(groupKey).poll(tagChannel);
    }*/

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
