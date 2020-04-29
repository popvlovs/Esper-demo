package espercep.demo.cases.ipc.chronicle;

import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song 2020/4/7
 */
public class ConsumerStoreFileListener implements StoreFileListener {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerStoreFileListener.class);
    private ExcerptTailer tailer;
    private String id;
    private CoordinatorStore coordinatorStore;

    public ExcerptTailer getTailer() {
        return tailer;
    }

    public void setTailer(ExcerptTailer tailer) {
        this.tailer = tailer;
    }

    public ConsumerStoreFileListener(String id, CoordinatorStore coordinatorStore) {
        this.id = id;
        this.coordinatorStore = coordinatorStore;
    }

    @Override
    public void onReleased(int cycle, File file) {
        try {
            logger.info("Released store file: {}", file.getCanonicalPath());
            if (tailer != null) {
                ConsumerMetaInfo consumer = coordinatorStore.getOrElse(id, ConsumerMetaInfo::new);
                consumer.setId(id);
                consumer.setHeartbeat(System.currentTimeMillis());
                consumer.setIndex(tailer.index());
                consumer.setReleasedStoreFile(file.getCanonicalPath());
                logger.info("Write to coordinator store: {}", consumer);
                coordinatorStore.register(consumer);
            }
        } catch (IOException e) {
            logger.info("Error on print store file released: ", e);
        }
    }

    @Override
    public void onAcquired(int cycle, File file) {
        try {
            logger.info("Acquired store file: {}", file.getCanonicalPath());
            if (tailer != null) {
                ConsumerMetaInfo consumer = coordinatorStore.getOrElse(id, ConsumerMetaInfo::new);
                consumer.setId(id);
                consumer.setHeartbeat(System.currentTimeMillis());
                consumer.setIndex(tailer.index());
                consumer.setAcquiredStoreFile(file.getCanonicalPath());
                logger.info("Write to coordinator store: {}", consumer);
                coordinatorStore.register(consumer);
            }
        } catch (IOException e) {
            logger.info("Error on print store file acquired: ", e);
        }
    }
}
