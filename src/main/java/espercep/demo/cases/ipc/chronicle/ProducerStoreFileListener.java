package espercep.demo.cases.ipc.chronicle;

import net.openhft.chronicle.queue.impl.StoreFileListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/4/7
 */
public class ProducerStoreFileListener implements StoreFileListener {
    private static final Logger logger = LoggerFactory.getLogger(ProducerStoreFileListener.class);
    private CoordinatorStore coordinatorStore;

    public ProducerStoreFileListener(CoordinatorStore coordinatorStore) {
        this.coordinatorStore = coordinatorStore;
    }

    @Override
    public void onReleased(int cycle, File file) {
        try {
            logger.info("Store file released: {}", file.getCanonicalPath());

            // Expiry purge: shared-memory
            File[] queueFiles = Optional.ofNullable(file.getParentFile().listFiles((dir, filename) -> filename.endsWith("cq4")))
                    .orElseGet(() -> new File[0]);
            Arrays.sort(queueFiles, Comparator.comparingLong(File::lastModified).reversed());
            ConsumerMetaInfo barrierConsumer = coordinatorStore.getReleasedStoreFileBarrier();
            logger.info("Find slowest consumer: {}", barrierConsumer);
            List<File> filesToDel = new ArrayList<>();
            for (int i = 5; i < queueFiles.length; ++i) {
                String filenameToDel = queueFiles[i].getCanonicalPath();
                if (barrierConsumer != null) {
                    if (filenameToDel.compareTo(barrierConsumer.getReleasedStoreFile()) > 0) {
                        logger.info("To retain: {}", filenameToDel);
                    } else {
                        logger.info("To delete: {}", filenameToDel);
                        filesToDel.add(queueFiles[i]);
                    }
                }
            }
            for (File fileToDel : filesToDel) {
                boolean succeed = fileToDel.delete();
                if (succeed) {
                    logger.info("Delete released store file success: {}", fileToDel.getCanonicalPath());
                } else {
                    logger.info("Delete released store file fail: {}", fileToDel.getCanonicalPath());
                }
            }
            logger.info("Available consumers: {}", coordinatorStore.availableConsumers());
        } catch (IOException e) {
            logger.info("Error on print store file released: ", e);
        }
    }

    @Override
    public void onAcquired(int cycle, File file) {

    }
}
