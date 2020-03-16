package espercep.demo.sharedbuffer;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/3/16
 */
public class NoLockQueue<E> implements Queue<E> {

    private RingBuffer<E> buffer;

    public static <T> NoLockQueue<T> build(int size, Class<T> tClass) {
        return new NoLockQueue<>(size, tClass);
    }

    private NoLockQueue(int size, Class<E> tClass) {
        buffer = RingBuffer.createSingleProducer(new EventFactory<E>() {
            @Override
            public E newInstance() {
                try {
                    return tClass.newInstance();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }, size, new BlockingWaitStrategy());
    }

    @Override
    public boolean add(E e) {
        long sequence = buffer.next();
        try {
            E cursor = buffer.get(sequence);

        } finally {

        }
        return false;
    }

    @Override
    public boolean offer(E e) {
        return false;
    }

    @Override
    public E remove() {
        return null;
    }

    @Override
    public E poll() {
        return null;
    }

    @Override
    public E element() {
        return null;
    }

    @Override
    public E peek() {
        return null;
    }

    @Override
    public int size() {
        return buffer.getBufferSize();
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean contains(Object o) {
        return false;
    }

    @Override
    public Iterator<E> iterator() {
        return null;
    }

    @Override
    public Object[] toArray() {
        return new Object[0];
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return null;
    }

    @Override
    public boolean remove(Object o) {
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return false;
    }

    @Override
    public void clear() {

    }
}
