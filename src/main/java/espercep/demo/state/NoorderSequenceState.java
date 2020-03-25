package com.hansight.hes.engine.ext.noorder.state;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/3/16
 */
public class NoorderSequenceState<E extends Map> {

    /**
     * 用于存储每个slot中的当前事件序列
     * 当所有slot中都有事件时，每个slot执行一次deque，其结果即为一次无序序列检测的输出序列
     * Map + LinkedList
     */
    private Map<Integer, Queue<E>> states;

    /**
     * 记录每个slot完成状况的bit set, 最多支持slot数为16
     * 完成记0，未完成记1
     * 例如0b1111000011表示在期待的10个事件中，已经有4个完成了
     */
    private volatile long bitState;

    /**
     * 无序序列检测规则支持的最大检测事件数
     */
    private volatile int numEventSlots;

    /**
     * state所属statement的规则ID
     */
    private int ruleId;

    private ReentrantLock lock = new ReentrantLock();

    public NoorderSequenceState(int numEventSlots, int ruleId) {
        if (numEventSlots > 32) {
            throw new RuntimeException("Noorder sequence events limit exceeded, max 32, current " + numEventSlots);
        }
        this.numEventSlots = numEventSlots;
        this.ruleId = ruleId;
        states = new ConcurrentHashMap<>();
        for (int i = 0; i < this.numEventSlots; i++) {
            states.put(i, new LinkedQueueWrapper<>(1024, i, this, lock));
        }
        bitState = (0b1 << numEventSlots) - 1;
    }

    @SuppressWarnings("unchecked")
    public void offer(Integer slot, E element) {
        lock.lock();
        try {
            Queue<E> queue = states.get(slot);
            queue.offer(element);
            EventBeanQueueState.addQueueState(ruleId, element, queue);
            if (queue.isEmpty()) {
                setBitState(slot, false);
            } else {
                setBitState(slot, true);
            }
        } finally {
            lock.unlock();
        }
    }

    public E poll(Integer slot) {
        lock.lock();
        try {
            Queue<E> queue = states.get(slot);
            E element = queue.poll();
            if (queue.isEmpty()) {
                setBitState(slot, false);
            } else {
                setBitState(slot, true);
            }
            EventBeanQueueState.removeQueueState(ruleId, element, queue);
            return element;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 每个slot触发一次deque，作为无序序列检出结果
     */
    public List<E> pollCompleteSequence() {
        lock.lock();
        try {
            List<E> completeSequence = new ArrayList<>();
            if (isComplete()) {
                for (int i = 0; i < this.numEventSlots; i++) {
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
    private void setBitState(int bit, boolean isComplete) {
        if (isComplete) {
            // 将第 tagChannel 位设置为0
            bitState = (~(0b1 << bit)) & bitState;
        } else {
            // 将第 tagChannel 位设置为1
            bitState = (0b1 << bit) | bitState;
        }
    }

    /**
     * 判断是否所有slot都已完成
     */
    private boolean isComplete() {
        return bitState == 0L;
    }

    public class LinkedQueueWrapper<E extends Map> extends LinkedBlockingQueue<E> {
        private int slot;
        private NoorderSequenceState<E> parentState;
        private ReentrantLock parentLock;

        public LinkedQueueWrapper(int capacity, int slot, NoorderSequenceState<E> parentState, ReentrantLock parentLock) {
            super(capacity);
            this.slot = slot;
            this.parentState = parentState;
            this.parentLock = parentLock;
        }

        public E pollAsParentState() {
            return this.parentState.poll(this.slot);
        }

        public void offerAsParentState(E element) {
            this.parentState.offer(this.slot, element);
        }

        public ReentrantLock acquireStateLock() {
            return parentLock;
        }
    }
}
