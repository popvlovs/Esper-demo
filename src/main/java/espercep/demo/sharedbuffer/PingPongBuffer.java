package espercep.demo.sharedbuffer;

import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/3/13
 */
public class PingPongBuffer {
    private static final int BUFFER_SIZE = 5000000;

    private List<List<Map>> buffer;
    private volatile int currentPingPong = 0;
    private volatile int consumerNum;
    private volatile CountDownLatch allConsumedCdt;
    private Map<Integer, Integer> consumedIndexTails;

    public PingPongBuffer(int consumerNum) {
        this.consumerNum = consumerNum;
        this.allConsumedCdt= new CountDownLatch(this.consumerNum);
        this.buffer = new ArrayList<>(2);
        this.consumedIndexTails = new HashMap<>(this.consumerNum);
        for (int i = 0; i < consumerNum; i++) {
            consumedIndexTails.put(i, 0);
        }

        // create ping
        this.buffer.add(new ArrayList<>(BUFFER_SIZE));
        // create pong
        this.buffer.add(new ArrayList<>(BUFFER_SIZE));
    }

    private synchronized void trySwitchPingPong() {
        try {
            allConsumedCdt.await();
            switchPingPong();
        } catch (InterruptedException e) {
            // ...
        }
    }

    private void switchPingPong() {
        if (currentPingPong == 0) {
            currentPingPong = 1;
        } else if (currentPingPong == 1) {
            currentPingPong = 0;
        } else {
            throw new RuntimeException("Error ping-pong index");
        }
        allConsumedCdt = new CountDownLatch(this.consumerNum);
        getPingBuffer().clear();
    }

    private List<Map> getPingBuffer() {
        return buffer.get(currentPingPong);
    }

    private List<Map> getPongBuffer() {
        return buffer.get((currentPingPong + 1) >> 1);
    }

    public void put(Map element) {
        List<Map> pingBuffer = getPingBuffer();
        if (pingBuffer.size() == BUFFER_SIZE) {
            // May blocking here
            trySwitchPingPong();
            pingBuffer = getPingBuffer();
        }

        if (pingBuffer.size() < BUFFER_SIZE) {
            pingBuffer.add(element);
        }
    }

    public Map get(int consumerId) {
        List<Map> pongBuffer = getPongBuffer();
        int tail = consumedIndexTails.get(consumerId);
        // TODO, pending to await pong buffer ready
        if (tail < BUFFER_SIZE) {
            consumedIndexTails.put(consumerId, tail+1);
        } else {
            consumedIndexTails.put(consumerId, 0);
            allConsumedCdt.countDown();
        }
        return pongBuffer.get(tail);
    }
}
