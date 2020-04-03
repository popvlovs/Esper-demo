package espercep.demo.cases.esper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.util.concurrent.CountDownLatch;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 * <p>
 * 使用自定义CountWindowGroupState实现count-window的demo
 *
 * @author yitian_song
 */
public class WindowCountMetric_Multithread_9 {
    private static final Logger logger = LoggerFactory.getLogger(WindowCountMetric_Multithread_9.class);

    public static void main(String[] args) throws Exception {
        // Create Esper engine(s)
        int threadNum = 8;
        long eventNum = 100_000_000L;

        CountDownLatch cdt = new CountDownLatch(threadNum);
        for (int i = 0; i < threadNum; ++i) {
            final int localIndex = i;
            new Thread(() -> {
                try {
                    long cnt = 0L;
                    long nanoTime = System.nanoTime();
                    MessageDigest digest = MessageDigest.getInstance("MD5");
                    while (cnt++ < eventNum) {
                        digest.digest(Long.toString(cnt).getBytes());
                    }
                    logger.info("Using nano time #{}: {}", localIndex, System.nanoTime() - nanoTime);
                } catch (Exception e) {
                    throw new RuntimeException("Error on execute eql", e);
                } finally {
                    cdt.countDown();
                }
            }).start();
        }
        cdt.await();
        System.exit(0);
    }
}
