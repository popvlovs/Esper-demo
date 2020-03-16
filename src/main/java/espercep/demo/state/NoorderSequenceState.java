package espercep.demo.state;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/3/16
 */
public class NoorderSequenceState<E extends Map> {

    private Map<Integer, Queue<E>> states;

    /**
     * 记录每个channel完成状况的bit set, 最多支持channel数为16
     * 完成记1，未完成记0
     * 例如0b1111000011表示有期待的10个事件中，已经有4个完成了
     */
    private volatile long bitState;

    /**
     * 无序检测规则中，支持同时发生的事件数
     */
    private volatile int numTagChannel;

    private ReentrantLock lock = new ReentrantLock();

    public NoorderSequenceState(int numTagChannel) {
        if (numTagChannel > 32) {
            throw new RuntimeException("Noorder sequence event types limit exceeded, max 32, current " + numTagChannel);
        }
        this.numTagChannel = numTagChannel;
        states = new ConcurrentHashMap<>();
        for (int i = 0; i < this.numTagChannel; i++) {
            states.put(i, new LinkedQueueWrapper<>(1024, i, this));
        }
        bitState = (0b1 << numTagChannel) - 1;
    }

    @SuppressWarnings("unchecked")
    public void offer(Integer channel, E element) {
        lock.lock();
        try {
            Queue<E> queue = states.get(channel);
            queue.offer(element);
            if (!element.containsKey("state_refs")) {
                element.put("state_refs", new HashSet<>());
            }
            ((Set) element.get("state_refs")).add(states.get(channel));
            if (queue.isEmpty()) {
                setBitState(channel, false);
            } else {
                setBitState(channel, true);
            }
        } finally {
            lock.unlock();
        }
    }

    public E poll(Integer channel) {
        lock.lock();
        try {
            Queue<E> queue = states.get(channel);
            E element = queue.poll();
            if (queue.isEmpty()) {
                setBitState(channel, false);
            } else {
                setBitState(channel, true);
            }
            if (element != null) {
                ((Set) element.get("state_refs")).remove(queue);
            }
            return element;
        } finally {
            lock.unlock();
        }
    }

    public List<E> pollCompleteSequence() {
        lock.lock();
        try {
            List<E> completeSequence = new ArrayList<>();
            if (isComplete()) {
                for (int i = 0; i < this.numTagChannel; i++) {
                    completeSequence.add(this.poll(i));
                }
                return completeSequence;
            } else {
                return null;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 设置完成标志到 bit set
     */
    private void setBitState(int tagChannel, boolean isComplete) {
        if (isComplete) {
            // 将第 tagChannel 位设置为0
            bitState = (~(0b1 << tagChannel)) & bitState;
        } else {
            // 将第 tagChannel 位设置为1
            bitState = (0b1 << tagChannel) | bitState;
        }
    }

    /**
     * 判断是否所有channel都已完成
     */
    private boolean isComplete() {
        return bitState == 0L;
    }

    public class LinkedQueueWrapper<E extends Map> extends LinkedBlockingQueue<E> {
        private int channel;
        private NoorderSequenceState<E> parentState;

        public LinkedQueueWrapper(int capacity, int channel, NoorderSequenceState<E> parentState) {
            super(capacity);
            this.channel = channel;
            this.parentState = parentState;
        }

        public E pollAsParentState() {
            return this.parentState.poll(this.channel);
        }

        public void offerAsParentState(E element) {
            this.parentState.offer(this.channel, element);
        }
    }
}
