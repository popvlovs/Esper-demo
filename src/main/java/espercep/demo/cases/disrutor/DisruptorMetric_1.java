package espercep.demo.cases.disrutor;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import espercep.demo.util.ArgsUtil;
import espercep.demo.util.CmdLineOptions;
import espercep.demo.util.MetricUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 */
public class DisruptorMetric_1 {

    private static final Logger logger = LoggerFactory.getLogger(DisruptorMetric_1.class);
    private static CmdLineOptions options;

    public static void main(String[] args) throws Exception {
        options = ArgsUtil.getArg(args);
        logger.info("Using args as {}", options);

        final List<Consumer> workerHandlers = new ArrayList<>(options.getCoreNum());
        for (int engineIndex = 0; engineIndex < options.getCoreNum(); ++engineIndex) {
            try {
                workerHandlers.add(new Consumer(engineIndex));
            } catch (Exception e) {
                throw new RuntimeException("Error on execute eql", e);
            }
        }

        // 2. 创建 Disruptor 用于线程间通信
        // Send event to consumer thread
        Disruptor<LongEventBeanWrapper> disruptor = new Disruptor<>(new EventFactory<LongEventBeanWrapper>() {
            @Override
            public LongEventBeanWrapper newInstance() {
                return new LongEventBeanWrapper();
            }
        }, 2 << 13, Executors.defaultThreadFactory(), ProducerType.SINGLE, new YieldingWaitStrategy());
        disruptor.handleEventsWith(workerHandlers.toArray(new Consumer[]{}));
        disruptor.start();

        // 3. 持续生成
        sendRandomEvents(disruptor);
    }

    private static void sendRandomEvents(Disruptor disruptor) {
        long remainingEvents = Long.MAX_VALUE;
        while (--remainingEvents > 0) {
            /* 这里有一个很有趣的现象：
             * 当不计算随机数时，整体的eps为100w；
             * 而计算随机数时，按理需要花费更多的cpu，性能应该下降才对，然而实际上并没有下降；
             * 猜测的原因如下，不计算随机数 -> publish速度太快 -> 消费者线程跟不上 -> 生产者线程经常parkNano -> 性能下降
             * 而计算随机数时 -> publish速度较慢 -> 生产者消费者较为均衡 -> 生产者线程能够非阻塞式的发出数据 -> 性能上升*/
            int randomVal = new Random().nextInt(2);
            MetricUtil.getConsumeRateMetric().mark();

            disruptor.publishEvent(new EventTranslator<LongEventBeanWrapper>() {
                @Override
                public void translateTo(LongEventBeanWrapper map, long sequence) {
                    map.setValue(sequence);
                }
            });
        }
    }

    private static class LongEventBeanWrapper {
        private long value;

        public long getValue() {
            return value;
        }

        public void setValue(long value) {
            this.value = value;
        }
    }

    public static class Consumer implements EventHandler<LongEventBeanWrapper> {
        private int consumerId;

        public Consumer(int consumerId) {
            this.consumerId = consumerId;
        }

        @Override
        public void onEvent(LongEventBeanWrapper map, long sequence, boolean endOfBatch) throws Exception {
            MetricUtil.getCounter("total consumed engine#" + this.consumerId).inc();
            // engine.sendEvent(map.getInnerMap(), "TestEvent");
            TimeUnit.NANOSECONDS.toMillis(1);
        }
    }
}
