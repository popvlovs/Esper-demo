package espercep.demo.cases;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/4/13
 */
public class WorkerLogger {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger("com.hansight.hes.metrics.MetricsCenter#*");
        logger.info("Hello");
    }
}
